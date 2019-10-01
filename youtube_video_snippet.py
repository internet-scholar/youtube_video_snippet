import argparse
import boto3
from internet_scholar import read_dict_from_s3_url, AthenaLogger, AthenaDatabase, compress
import logging
import googleapiclient.discovery
from googleapiclient.errors import HttpError
import csv
from pathlib import Path
import json
from datetime import datetime
import time
import uuid

SELECT_TWITTER_STREAM_VIDEO = """
select distinct
  url_extract_parameter(validated_url, 'v') as video_id,
  1 as twitter_stream,
  0 as youtube_related_video,
  0 as twitter_search
from
  validated_url
where
  url_extract_host(validated_url) = 'www.youtube.com'
"""

EXTRA_TWITTER_STREAM_VIDEO = """
  and url_extract_parameter(validated_url, 'v') not in (select id from youtube_video_snippet)
"""

SELECT_YOUTUBE_RELATED_VIDEO = """
select distinct
  id.videoId as video_id,
  0 as twitter_stream,
  1 as youtube_related_video,
  0 as twitter_search
from
  youtube_related_video
where
  created_at = cast(current_date as varchar)
"""

EXTRA_YOUTUBE_RELATED_VIDEO = """
  and id.videoId not in (select id from youtube_video_snippet)
"""

SELECT_GROUP_BY = """
with group_by_table as (
{}
)
select
  video_id,
  sum(twitter_stream) as twitter_stream,
  sum(youtube_related_video) as youtube_related_video,
  sum(twitter_search) as twitter_search
from
  group_by_table
group by
  video_id
"""

SELECT_COUNT = """
with count_table as (
{}
)
select
  count(distinct video_id) as video_count
from
  count_table
"""

CREATE_VIDEO_SNIPPET_JSON = """
create external table if not exists youtube_video_snippet
(
    kind string,
    etag string,
    id   string,
    retrieved_at timestamp,
    source struct<
        twitter_stream: boolean,
        youtube_related_video: boolean,
        twitter_search: boolean
    >,
    snippet struct<
        publishedAt:  timestamp,
        title:        string,
        description:  string,
        channelId:    string,
        channelTitle: string,
        categoryId:   string,
        tags:         array<string>,
        liveBroadcastContent: string,
        defaultlanguage:      string,
        defaultAudioLanguage: string,
        localized:  struct <title: string, description: string>,
        thumbnails: struct<
            default:  struct <url: string, width: int, height: int>,
            medium:   struct <url: string, width: int, height: int>,
            high:     struct <url: string, width: int, height: int>,
            standard: struct <url: string, width: int, height: int>,
            maxres:   struct <url: string, width: int, height: int>
        >
    >
)
PARTITIONED BY (creation_date String)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1',
    'ignore.malformed.json' = 'true'
)
LOCATION 's3://{s3_bucket}/youtube_video_snippet/'
TBLPROPERTIES ('has_encrypted_data'='false')
"""


class YoutubeVideoSnippet:
    def __init__(self, credentials, athena_data, s3_admin, s3_data):
        self.credentials = credentials
        self.athena_data = athena_data
        self.s3_admin = s3_admin
        self.s3_data = s3_data

    LOGGING_INTERVAL = 100
    WAIT_WHEN_SERVICE_UNAVAILABLE = 30

    def collect_video_snippets(self):
        logging.info("Start collecting video snippets")
        athena = AthenaDatabase(database=self.athena_data, s3_output=self.s3_admin)
        if not athena.table_exists("youtube_video_snippet"):
            select_twitter_stream_video = SELECT_TWITTER_STREAM_VIDEO
            select_youtube_related_video = SELECT_YOUTUBE_RELATED_VIDEO
        else:
            logging.info("Table youtube_video_snippet exists")
            select_twitter_stream_video = SELECT_TWITTER_STREAM_VIDEO + EXTRA_TWITTER_STREAM_VIDEO
            select_youtube_related_video = SELECT_YOUTUBE_RELATED_VIDEO + EXTRA_YOUTUBE_RELATED_VIDEO
        queries = [select_twitter_stream_video]
        if athena.table_exists("youtube_related_video"):
            queries.append(select_youtube_related_video)
        query = " union all ".join(queries)
        query_count = SELECT_COUNT.format(query)
        query_group_by = SELECT_GROUP_BY.format(query)
        logging.info("Download IDs for all Youtube videos that have not been processed yet")
        video_count = int(athena.query_athena_and_get_result(query_string=query_count)['video_count'])
        logging.info("There are %d links to be processed: download them", video_count)
        video_ids_csv = athena.query_athena_and_download(query_string=query_group_by, filename="video_ids.csv")

        output_json = Path(Path(__file__).parent, 'tmp', 'youtube_video_snippet.json')
        Path(output_json).parent.mkdir(parents=True, exist_ok=True)
        current_key = 0
        youtube = googleapiclient.discovery.build(serviceName="youtube",
                                                  version="v3",
                                                  developerKey=
                                                  self.credentials[current_key]['developer_key'],
                                                  cache_discovery=False)
        with open(video_ids_csv, newline='') as csv_reader:
            with open(output_json, 'w') as json_writer:
                reader = csv.DictReader(csv_reader)
                num_videos = 0
                for video_id in reader:
                    if num_videos % self.LOGGING_INTERVAL == 0:
                        logging.info("%d out of %d videos processed", num_videos, video_count)
                    num_videos = num_videos + 1

                    service_unavailable = 0
                    no_response = True
                    while no_response:
                        try:
                            response = youtube.videos().list(part="snippet",id=video_id['video_id']).execute()
                            no_response = False
                        except HttpError as e:
                            if "403" in str(e):
                                logging.info("Invalid {} developer key: {}".format(
                                    current_key,
                                    self.credentials[current_key]['developer_key']))
                                current_key = current_key + 1
                                if current_key >= len(self.credentials):
                                    raise
                                else:
                                    youtube = googleapiclient.discovery.build(serviceName="youtube",
                                                                              version="v3",
                                                                              developerKey=
                                                                              self.credentials[current_key][
                                                                                  'developer_key'],
                                                                              cache_discovery=False)
                            elif "503" in str(e):
                                logging.info("Service unavailable")
                                service_unavailable = service_unavailable + 1
                                if service_unavailable <= 10:
                                    time.sleep(self.WAIT_WHEN_SERVICE_UNAVAILABLE)
                                else:
                                    raise
                            else:
                                raise
                    if len(response.get('items', [])) == 0:
                        response['id'] = video_id['video_id']
                        response['retrieved_at'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                        response['description'] = "Video unavailable. It has probably been removed by the user."
                        response['source'] = dict()
                        response['source']['twitter_stream'] = True if video_id['twitter_stream'] == 1 else False
                        response['source']['youtube_related_video'] = True if video_id['youtube_related_video'] == 1 else False
                        response['source']['twitter_search'] = True if video_id['twitter_search'] == 1 else False
                        json_writer.write("{}\n".format(json.dumps(response)))
                    else:
                        for item in response['items']:
                            item['snippet']['publishedAt'] = item['snippet']['publishedAt'].rstrip('Z').replace('T', ' ')
                            item['retrieved_at'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                            item['source'] = dict()
                            item['source']['twitter_stream'] = True if video_id['twitter_stream'] == 1 else False
                            item['source']['youtube_related_video'] = True if video_id['youtube_related_video'] == 1 else False
                            item['source']['twitter_search'] = True if video_id['twitter_search'] == 1 else False
                            json_writer.write("{}\n".format(json.dumps(item)))

        logging.info("Compress file %s", output_json)
        compressed_file = compress(filename=output_json, delete_original=True)

        s3 = boto3.resource('s3')
        s3_filename = "youtube_video_snippet/created_at={}/{}-{}.json.bz2".format(datetime.utcnow().strftime("%Y-%m-%d"),
                                                                               uuid.uuid4().hex,
                                                                               num_videos)
        logging.info("Upload file %s to bucket %s at %s", compressed_file, self.s3_data, s3_filename)
        s3.Bucket(self.s3_data).upload_file(str(compressed_file), s3_filename)

        logging.info("Recreate table for Youtube channel stats")
        athena.query_athena_and_wait(query_string="DROP TABLE IF EXISTS youtube_video_snippet")
        athena.query_athena_and_wait(query_string=CREATE_VIDEO_SNIPPET_JSON.format(s3_bucket=self.s3_data))
        athena.query_athena_and_wait(query_string="MSCK REPAIR TABLE youtube_video_snippet")

        logging.info("Concluded collecting video snippets")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='S3 Bucket with configuration', required=True)
    args = parser.parse_args()

    config = read_dict_from_s3_url(url=args.config)
    logger = AthenaLogger(app_name="youtube",
                          s3_bucket=config['aws']['s3-admin'],
                          athena_db=config['aws']['athena-admin'])
    try:
        youtube_video_snippet = YoutubeVideoSnippet(credentials=config['youtube'],
                                                    athena_data=config['aws']['athena-data'],
                                                    s3_admin=config['aws']['s3-admin'],
                                                    s3_data=config['aws']['s3-data'])
        youtube_video_snippet.collect_video_snippets()
    finally:
        logger.save_to_s3()
        logger.recreate_athena_table()


if __name__ == '__main__':
    main()
