from pathlib import Path
import scrapy
import json
from jobs_project.items import JobsProjectItem

class Jobpider(scrapy.Spider):
    name = 'job_spider'  # Name of the spider
    custom_settings = {
        'ITEM_PIPELINES': {
            'jobs_project.pipelines.JobsProjectPipeline': 300,  # Item pipeline configuration
        },
    }

    def __init__(self, **kwargs):
        # Initializes the spider with any additional arguments passed during runtime
        super().__init__(**kwargs)

    def start_requests(self):
        # List of URLs to start scraping
        urls = [
            'file:///app/data/s01.json',  # Path to the first JSON file
            'file:///app/data/s02.json',  # Path to the second JSON file
        ]
        
        # Iterate over the URLs and generate requests for each
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_page)

    def parse_page(self, response):
        # Parse the JSON response and extract job data
        data = json.loads(response.text)

        # Loop through the jobs in the JSON data
        for job in data.get('jobs', []):  # Ensure 'jobs' key exists
            job_data = job.get('data', {})  # Get the job details
            
            # Create an item to store the job data
            item = JobsProjectItem()
            
            # Extract relevant fields from the job data and assign to item
            item['slug'] = job_data.get('slug')
            item['language'] = job_data.get('language')
            item['languages'] = job_data.get('languages')
            item['req_id'] = job_data.get('req_id')
            item['title'] = job_data.get('title')
            item['description'] = job_data.get('description')
            item['street_address'] = job_data.get('street_address')
            item['city'] = job_data.get('city')
            item['state'] = job_data.get('state')
            item['country_code'] = job_data.get('country_code')
            item['postal_code'] = job_data.get('postal_code')
            item['location_type'] = job_data.get('location_type')
            item['latitude'] = job_data.get('latitude')
            item['longitude'] = job_data.get('longitude')
            item['categories'] = job_data.get('categories')
            item['tags'] = job_data.get('tags')
            item['tags5'] = job_data.get('tags5')
            item['tags6'] = job_data.get('tags6')
            item['brand'] = job_data.get('brand')
            item['promotion_value'] = job_data.get('promotion_value')
            item['salary_currency'] = job_data.get('salary_currency')
            item['salary_value'] = job_data.get('salary_value')
            item['salary_min_value'] = job_data.get('salary_min_value')
            item['salary_max_value'] = job_data.get('salary_max_value')
            item['benefits'] = job_data.get('benefits')
            item['employment_type'] = job_data.get('employment_type')
            item['hiring_organization'] = job_data.get('hiring_organization')
            item['source'] = job_data.get('source')
            item['apply_url'] = job_data.get('apply_url')
            item['internal'] = job_data.get('internal')
            item['searchable'] = job_data.get('searchable')
            item['applyable'] = job_data.get('applyable')
            item['li_easy_applyable'] = job_data.get('li_easy_applyable')
            item['ats_code'] = job_data.get('ats_code')
            item['update_date'] = job_data.get('update_date')
            item['create_date'] = job_data.get('create_date')
            item['category'] = job_data.get('category')
            item['full_location'] = job_data.get('full_location')
            item['short_location'] = job_data.get('short_location')

            # Yield the item to be processed further
            yield item