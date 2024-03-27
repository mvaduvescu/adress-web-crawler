import fs from 'fs';
import parquetjs from "@dsnp/parquetjs";
import axios from "axios";

const reader = await parquetjs.ParquetReader.openFile("company_list.parquet");
const cursor = reader.getCursor();
let record = null;

const domainList = [];

while ((record = await cursor.next())) {
    const domain = record.domain;
    domainList.push(domain);
}

const successfulData = [];
const failedFetches = [];

async function fetchAndParseInOrder(domains) {
    let successfulFetchCount = 0;
    let failedFetchCount = 0;

    for (const domain of domains) {
        const result = await fetchAndParse(domain);
        if (result) {
            successfulFetchCount++;
            console.log("Domains successfully fetched:", successfulFetchCount);
            console.log("Domains failed to fetch:", failedFetchCount);
            console.log("Total domains:", domains.length)
            console.log("-------------------------------------------------");
        } else {
            failedFetchCount++;
            failedFetches.push({ domain }); // Pushing domain name only
            console.log("Domains successfully fetched:", successfulFetchCount);
            console.log("Domains failed to fetch:", failedFetchCount);
            console.log("Total domains:", domains.length)
            console.log("-------------------------------------------------");
        }
    }

    // Write failed fetches to a JSON file
    fs.writeFileSync('failed_data.json', JSON.stringify(failedFetches, null, 2));
}

async function fetchAndParse(domain) {
    const maxRetries = 4;
    let retries = 0;

    while (retries < maxRetries) {
        try {
            let response;
            let protocol = 'http';

            // Try HTTP first, then HTTPS if it fails twice
            if (retries >= 2) {
                protocol = 'https';
            }

            response = await axios.get(`${protocol}://${domain}`, { timeout: 5000 });

            const html = response.data;
            const addressRegex = /\d+\s[A-Za-z0-9\s\.\']+,\s[A-Za-z\s]+,\s[A-Za-z]+\s\d{5}/g; // Regex pattern for the provided address format
            const matches = Array.from(html.matchAll(addressRegex), match => match[0].trim()); // Extract all matches and trim each address

            if (matches.length >= 1) {
                console.log(`Domain: ${domain}`);
                console.log("Addresses extracted:", matches);
                console.log("--------------------");

                // Store successful fetches data
                successfulData.push({
                    domain,
                    addresses: matches
                });

                // Write successful data to a JSON file
                fs.writeFileSync('successful_data.json', JSON.stringify(successfulData, null, 2));
            }

            return true;

        } catch (error) {
            retries++;
            console.error(`Retry ${retries}: Failed to fetch data from: ${domain}`);
            if (retries === maxRetries) {
                console.error(`Exceeded maximum retries for ${domain}`);
                console.error(`------------------------------------`);
                return false;
            }
        }
    }
}

await fetchAndParseInOrder(domainList);
