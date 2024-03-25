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

async function fetchAndParseInOrder(domains) {
  let successfulFetchCount = 0;
  let failedFetchCount = 0;

  for (const domain of domains) {
    const result = await fetchAndParse(domain);
    if (result) {
      successfulFetchCount++;
    } else {
      failedFetchCount++;
    }
  }

  console.log("Total domains successfully fetched:", successfulFetchCount);
  console.log("Total domains failed to fetch:", failedFetchCount);
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
        return match && match[0].length <= 150 ? match[0] : "N/A";
      };

      const cityRegex = /\b(?:city|town|village)\b\s*[,:]\s*([^\n]+)/i;
      const postcodeRegex = /\b\d{5}\b/;
      const roadRegex = /\b(?:street|road|avenue|lane|drive)\b\s*[,:]\s*([^\n]+)/i;
      const roadNumberRegex = /\b\d{1,4}(?:\s*[a-zA-Z])?\b/;
      const countryRegex = /\b(?:country|nation)\b\s*[,:]\s*([^\n]+)/i;
      const regionRegex = /\b(?:region|state|province)\b\s*[,:]\s*([^\n]+)/i;

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
      console.log(`City: ${city}`);
      console.log(`Postcode: ${postcode}`);
      console.log(`Road: ${road}`);
      console.log(`Road Numbers: ${roadNumbers}`);
      console.log(`Country: ${country}`);
      console.log(`Region: ${region}`);
      console.log("--------------------");

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


