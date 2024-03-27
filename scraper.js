// Description: A script to scrape addresses from company websites.

// Import the necessary modules
import fs from 'fs';
import parquetjs from "@dsnp/parquetjs";
import axios from "axios";
import { parse as parseUrl } from 'url';
import { parse } from 'node-html-parser';

const domainsToProcessNumber = 2437; // MODIFY THIS VALUE TO CHANGE THE NUMBER OF DOMAINS TO PROCESS
const timeoutDuration = 5000; // MODIFY THIS VALUE TO CHANGE THE TIMEOUT DURATION
const maxRetryUntilSuccess = 4; // MODIFY THIS VALUE TO CHANGE THE MAXIMUM NUMBER OF RETRIES - WILL IMPROVE PERFORMANCE

// Open the Parquet file
const reader = await parquetjs.ParquetReader.openFile("company_list.parquet");

// Get the cursor to iterate over the records
const cursor = reader.getCursor();

// Define a variable to store the current record
let record = null;

// Define an array to store the domain names
const domainList = [];

// Iterate over the records and extract the domain names
while ((record = await cursor.next())) {
    // Get the domain name from the record
    const domain = record.domain;
    // Add the domain name to the list
    domainList.push(domain);
}

// Define an array to store the successful data
const successfulData = [];
// Define an array to store the failed fetches
const failedFetches = [];
// Define an array to store the failed addresses
const failedAddresses = [];

// Function to fetch and parse the data from the domains
async function fetchAndParseInOrder(domains) {
    // Define counters for successful and failed fetches
    let successfulFetchCount = 0;
    // Define counters for failed fetch and failed address
    let failedFetchCount = 0;
    // Define counter for failed address
    let failedAddressCount = 0;

    // Limit the loop to iterate over the first x domains
    const domainsToProcess = domains.slice(0, domainsToProcessNumber);

    // Iterate over the domains and fetch the data
    for (let i = 0; i < domainsToProcess.length; i++) {
        // Get the domain name
        const domain = domainsToProcess[i];
        // Calculate the remaining domains
        const remainingDomains = domainsToProcess.length - i;
        // Fetch and parse the data from the domain
        const result = await fetchAndParse(domain);
        // Check the result of the fetch and update the counters
        if (result.success) {
            // Increment the successful fetch count
            successfulFetchCount++;
            // Log the successful fetch
            console.log("Domains successfully fetched:", successfulFetchCount);
            console.log("Domains failed to fetch:", failedFetchCount);
            console.log("Remaining domains:", remainingDomains);
            console.log("-------------------------------------------------");
        // If the fetch failed
        } else {
            // Increment the failed fetch count
            if (result.failedToFetch) {
                failedFetchCount++;
            }
            // Increment the failed address count
            if (result.failedToGetAddress) {
                // Attempt to find contact page if address not found
                console.log(`Address not found for ${domain}. Attempting to find contact page...`);
                // Find the contact page URL
                const contactPageUrl = await findContactPage(domain);
                // If a contact page URL is found
                if (contactPageUrl) {
                    // Log the contact page URL
                    console.log(`Contact page found for ${domain}: ${contactPageUrl}`);
                    // Retry fetching and parsing with contact page URL
                    const contactPageResult = await fetchAndParse(contactPageUrl);
                    // Update the counters based on the result
                    if (contactPageResult.success) {
                        // Increment the successful fetch count
                        successfulFetchCount++;
                    // If the fetch failed
                    } else {
                        // Increment the failed fetch count
                        failedFetchCount++;
                    }
                // If no contact page URL is found
                } else {
                    // Increment the failed address count
                    console.error(`No contact page found for ${domain}`);
                    // Increment the failed address count
                    failedAddressCount++;
                }
            }
            // Log the failed fetch
            failedFetches.push({ domain }); // Pushing domain name only
            // Log the failed fetch
            console.log("Domains successfully fetched:", successfulFetchCount);
            console.log("Domains failed to fetch:", failedFetchCount);
            console.log("Remaining domains:", remainingDomains);
            console.log("-------------------------------------------------");
            // Write failed data to a JSON file
            fs.writeFileSync('failed_data.json', JSON.stringify(failedFetches, null, 2));
        }
    }

    // Write successful data to a JSON file after all domains have been processed
    fs.writeFileSync('successful_data.json', JSON.stringify(successfulData, null, 2));

    // Write log to a text file after all domains have been processed
    const logContent =`
        Successful domains: ${successfulFetchCount}
        Failed to fetch domains: ${failedFetchCount}
        Failed to get address domains: ${failedAddressCount}
        Total domains: ${domainsToProcess.length}
    `;
    // Write the log content to a text file
    fs.writeFileSync('fetch_log.txt', logContent);
}


// Function to fetch and parse the data from a domain
async function fetchAndParse(domain) {
    // Define the maximum number of retries
    const maxRetries = maxRetryUntilSuccess;
    // Define the initial number of retries
    let retries = 0;

    // Attempt to fetch the data from the domain
    while (retries < maxRetries) {
        // Try to fetch the data
        try {
            // Fetch the data from the domain
            let response;
            // Define the protocol to use
            let protocol = 'http';

            // Try HTTP first, then HTTPS if it fails twice
            if (retries >= 2) {
                // Use HTTPS after two retries
                protocol = 'https';
            }

            // Fetch the data from the domain using the specified protocol
            response = await axios.get(`${protocol}://${domain}`, { timeout: timeoutDuration });

            // Parse the HTML content to extract the text
            const html = response.data;

            // Extract the text content from the HTML
            const text = parse(html).text.replaceAll(/(\n+|\r+|\t+|\s+)/gmi, ' ').trim();

            // Define regular expression to extract addresses
            let addresses = [];
            
            // Define regular expressions for different countries
            const usAddressRegex = /\b\d+\s[A-Za-z\s]+,\s[A-Za-z\s]+\s\d{5}(?:-\d{4})?\b/g;
            const ukAddressRegex = /\b\d+\s[A-Za-z\s]+,\s[A-Za-z\s]+\b(?:,\s[A-Za-z]+\b)?\s[A-Za-z]+\s\d{1,2}[A-Za-z]?\s\d[A-Za-z]{2}\b/g;
            const germanyAddressRegex = /\b\d{5}\s[A-Za-z\s]+,\s\d+\b/g;
            const canadaAddressRegex = /\b\d+\s[A-Za-z\s]+,\s[A-Za-z\s]+\s[A-Za-z]+\s\d[A-Za-z]\d\b/g;
            const franceAddressRegex = /\b\d+\s[A-Za-z\s]+,\s\d{5}\s[A-Za-z]+\b/g;
            const australiaAddressRegex = /\b\d+\s[A-Za-z\s]+,\s[A-Za-z\s]+\s[A-Za-z]+\s\d{4}\b/g;
            const japanAddressRegex = /\d{3}-\d{4}\s[A-Za-z\s]+\d{1}-\d{2}-\d{2}/g;
            const switzerlandAddressRegex = /\b\d+\s[A-Za-z\s]+,\s\d{4}\s[A-Za-z]+\b/g;

            // Combine all regex into an array for easier iteration
            const addressRegexes = [
                usAddressRegex, 
                ukAddressRegex,
                 germanyAddressRegex, 
                 canadaAddressRegex, 
                 franceAddressRegex,
                  australiaAddressRegex, 
                  japanAddressRegex, 
                  switzerlandAddressRegex
                ];

            // Iterate through each regex and try to find addresses
            for (const regex of addressRegexes) {
                // Extract addresses using the regex
                addresses = Array.from(text.matchAll(regex), match => match[0].trim());
                // Break the loop if addresses are found
                if (addresses.length > 0) {
                    break; // Stop iterating if addresses are found
                }
            }

            // Log the domain and addresses
            if (addresses.length >= 1) {
                // Log the domain and addresses
                console.log(`Domain: ${domain}`);
                console.log("Addresses extracted:", addresses);
                console.log("--------------------");

                // Store successful fetches data
                successfulData.push({
                    domain,
                    addresses
                });

                // Write successful data to a JSON file
                return { success: true };
            // If no addresses are found
            } else {
                // Log the domain and error message
                console.error(`No address found for ${domain}`);
                // Write failed data to a JSON file
                fs.writeFileSync('failed_data.json', JSON.stringify(failedFetches, null, 2));
                // Store failed addresses
                failedAddresses.push({ domain }); // Pushing domain name only
                // Write failed data to a JSON file
                return { success: false, failedToGetAddress: true };
            }

        // Catch any errors that occur during the fetch
        } catch (error) {
            // Increment the number of retries
            retries++;
            // Log the error message
            console.error(`Retry ${retries}: Failed to fetch data from: ${domain}`);
            // Log the error message
            if (retries === maxRetries) {
                // Log the error message
                console.error(`Exceeded maximum retries for ${domain}`);
                console.error(`------------------------------------`);
                // Store failed fetches
                // Log the failed fetch
                return { success: false, failedToFetch: true };
            }
        }
    }
}

// Function to find the contact page URL
async function findContactPage(domain) {
    // Try to fetch the data from the domain
    try {
        // Fetch the data from the domain
        const response = await axios.get(`http://${domain}`, { timeout: timeoutDuration });
        // Parse the HTML content to extract the contact page URL
        const html = response.data;
        
        // Define regex to find links or buttons related to contact pages
        const contactPageRegex = /<a[^>]*\b(?:contact|kontakt|contact us|kontaktieren|contacto|contactez-nous|contato)\b[^>]*>(.*?)<\/a>/gi;
        // Match all instances of the contact page regex
        const matches = html.matchAll(contactPageRegex);
        
        // Iterate over all matches and extract the URLs
        for (const match of matches) {
            // Define regex to extract the URL from the match
            const urlRegex = /href=["'](.*?)["']/i;
            // Match the URL regex against the match
            const urlMatch = urlRegex.exec(match[0]);
            // If a URL is found, return it
            if (urlMatch && urlMatch[1]) {
                // Extract the contact page URL
                const contactPageUrl = urlMatch[1];
                // Log the contact page URL
                return contactPageUrl;
            }
        }
        
        return null; // Return null if no contact page URL is found
    // Catch any errors that occur during the fetch
    } catch (error) {
        // Log the error message
        console.error(`Error while fetching contact page for ${domain}: ${error.message}`);
        // Return null if an error occurs
        return null;
    }
}

// Call the function to fetch and parse the data from the domains
await fetchAndParseInOrder(domainList);
