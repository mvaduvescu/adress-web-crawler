import fs from 'fs';
import parquetjs from "@dsnp/parquetjs";
import axios from "axios";
import cheerio from "cheerio";

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
      const $ = cheerio.load(html);

      const findInfo = (regex) => {
        const text = $("body").text();
        const match = text.match(regex);
        return match && match[1] && match[1].length <= 150 ? match[1].trim() : "N/A";
      };

      const locationRegex = /\b((?:[a-zA-Z\s]+\b(?:straße|weg|allee|platz|street|road|avenue|lane|drive|boulevard)\b\s*[,:]\s*\d{1,4}(?:\s*[a-zA-Z])?)|(?:[a-zA-Z\s]+\b(?:region|state|province|bundesland|kanton)\b\s*[,:]\s*[a-zA-Z\s]+)|(?:[a-zA-Z\s]+\b(?:country|nation|land)\b\s*[,:]\s*[a-zA-Z\s]+))/ig;

      const cityRegex = /\b(?:city|stadt|town|ort|village|dorf)\b\s*[,:]\s*([^\n]+)/i;
      const postcodeRegex = /\b(?:zip|zipcode|postal\s?code|plz|postleitzahl)\b\s*[,:]\s*([^\n]+)/i;
      const roadRegex = /\b(?:street|st\.|strasse?|str\.|road|r\.|avenue|ave\.|lane|ln\.|drive|dr\.|boulevard|blvd\.|way|wy\.|platz|platz\.|platzweg)\b\s*[,:]\s*([^\n]+)/i;
      const roadNumberRegex = /\b\d{1,4}(?:\s*[a-zA-Z])?\b/;
      const countryRegex = /\b(?:country|nation|land)\b\s*[,:]\s*([^\n]+)/i;
      const regionRegex = /\b(?:region|state|province|bundesland)\b\s*[,:]\s*([^\n]+)/i;

      
      let location = findInfo(locationRegex);
      let city = findInfo(cityRegex);
      let postcode = findInfo(postcodeRegex);
      let road = findInfo(roadRegex);
      let roadNumbers = findInfo(roadNumberRegex);
      let country = findInfo(countryRegex);
      let region = findInfo(regionRegex);

      if (
        city === "N/A" ||
        postcode === "N/A" ||
        road === "N/A" ||
        roadNumbers === "N/A" ||
        country === "N/A" ||
        region === "N/A"
      ) {
        const contactPageResponse = await axios.get(
          `${protocol}://${domain}/contact`,
          { timeout: 5000 }
        );
        const contactHtml = contactPageResponse.data;
        const $contact = cheerio.load(contactHtml);

        const basicContactPagesRegexes = [
          /\/?contact[\-a-z]*(\.html?)?/i,
          /\/?about[\-a-z]*(\.html?)?/i,
          /\/?kontakt[\-a-z]*(\.html?)?/i,
        ];

        const contactPhrasesRegex = [
          /start now/i,
          /contact us/i,
          /get a quote/i,
          /jetzt starten/i,
          /kontaktieren sie uns/i,
          /erhalten sie ein angebot/i,
        ];

        const isContactPage = basicContactPagesRegexes.some((regex) =>
          regex.test(contactPageResponse.config.url)
        );

        const isContactPhraseFound = contactPhrasesRegex.some((regex) =>
          regex.test($contact("body").text())
        );

        if (isContactPage || isContactPhraseFound) {
          console.log("Information Received from Contact Tab");
        }
      }

      console.log(`Domain: ${domain}`);
      console.log(`Location: ${location}`);
      console.log(`City: ${city}`);
      console.log(`Postcode: ${postcode}`);
      console.log(`Road: ${road}`);
      console.log(`Road Numbers: ${roadNumbers}`);
      console.log(`Country: ${country}`);
      console.log(`Region: ${region}`);
      console.log("--------------------");

      // Store successful fetches data
      successfulData.push({
        domain,
        location,
        city,
        postcode,
        road,
        roadNumbers,
        country,
        region
      });

      // Write successful data to a JSON file
      fs.writeFileSync('successful_data.json', JSON.stringify(successfulData, null, 2));

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
