import pandas
from newsapi import NewsApiClient
from google.cloud import bigquery

client = bigquery.Client()
newsapi = NewsApiClient(api_key='347b0c89e130428aa97b966d840e5926')
dataset_id = "handy-cell-451408-a5.first_datasetin1"

def fetch_sources():
    science_sources = newsapi.get_sources(category='science', country='us')
    business_sources = newsapi.get_sources(category='business', country='us')

    sources = science_sources['sources'] + business_sources['sources']

    pandas.DataFrame(sources).to_csv("sources.csv", index=False)

def fetch_articles():
    sources = pandas.read_csv('sources.csv')['id'].tolist()

    to_date = datetime.now(UTC)
    from_date = to_date - timedelta(days=3)

    articles = []
    for i in sources:
        try:
            response = newsApi.get_everything(sort_by='popularity', from_param=from_date, to=to_date, page_size=100, sources=i)
            if response['status'] == 'ok':
                articles = articles + response['articles']
        except Exception as e:
            print(f'error {e} in fetching articles for {i}')

    data_frame = pandas.DataFrame(articles)
    data_frame.drop(columns=['urlToImage', 'content'], errors='ignore', inplace=True)
    data_frame.to_csv('articles.csv', index=False,)

def upload_csv_to_bigquery(file_path, table_id):
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        encoding="UTF-8",
    )
    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    job.result()

async def etl_pipeline(request):
    sources = await fetch_sources()
    articles = await fetch_articles()
    upload_csv_to_bigquery('sources.csv', dataset_id + ".sources")
    upload_csv_to_bigquery('articles.csv', dataset_id + ".articles")
    return "Done successfully, articles and sources table updated!"

