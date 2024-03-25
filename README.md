# adress-web-crawler
Tool for scrapping parquet files containing domain names for country, region, city, postcode, road, and road numbers. 

This is a web scraping tool built with Node.js that extracts address information from a list of company domains. It utilizes Axios for HTTP requests, Cheerio for HTML parsing, and ParquetJS for reading Parquet files. The tool fetches web pages, extracts address details such as city, postcode, road, country, and region using regular expressions, and identifies contact pages based on URL patterns and specific phrases. The extracted information is then logged for further analysis.
