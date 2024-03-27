# adress-web-crawler
Tool for scrapping parquet files containing domain names for country, region, city, postcode, road, and road numbers. 

## Overview
Address Web Scraper is a Node.js tool designed to extract address information from a list of company domains. It utilizes Axios for HTTP requests, Cheerio for HTML parsing, and ParquetJS for reading Parquet files. The tool fetches web pages, extracts address details such as city, postcode, road, country, and region using regular expressions, and identifies contact pages based on URL patterns and specific phrases. The extracted information is then logged for further analysis.

##Installation
To use the Address Web Scraper, ensure you have Node.js installed on your system. Then, install the dependencies by running:
```
npm install axios @dsnp/parquetjs cheerio node-html-parser
```

##Usage
To run the Address Web Scraper, execute the following command:
```
node scraper.js
```

> Ensure that you have a Parquet file named company_list.parquet containing domain names in the project directory. The tool will read this file and fetch information from the corresponding domains.

##Dependencies
The Address Web Scraper relies on the following dependencies:
```
axios	^1.6.8
@dsnp/parquetjs	^1.6.2
cheerio	^1.0.0-rc.12
node-html-parser	^6.1.12
```

## Project Structure
The project structure is as follows:
```
adress-web-scrapper/
│
├── scraper.js         # Main entry point of the application
├── clear_data.js      # Run this with node script.js to clear the logs and data created after running the program
├── company_list.parquet    # Parquet file containing domain names
├── failed_data.json   # JSON file storing failed fetches data
├── successful_data.json   # JSON file storing successful fetches data
├── fetch_log.txt      # Text file containing fetch log
├── package.json       # Project configuration and dependencies
└── node_modules/      # Installed dependencies (generated by npm)
```

## Output
The Address Web Scraper generates the following output files:
``
failed_data.json: JSON file containing data for domains that failed to fetch.
successful_data.json: JSON file containing data for successfully fetched domains.
fetch_log.txt: Text file containing a summary of fetch operations.
``

### License
This project is licensed under the MIT License.
