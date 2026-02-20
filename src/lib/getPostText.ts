import fs from "node:fs";
import { parse } from "csv-parse";
import { finished } from "stream/promises";
import _ from "lodash";
import { AtpAgent, Facet, RichText } from "@atproto/api";

// season,year,month,day,city,sport,event,url,medal,winner,country
interface Row {
	season: string;
	year: string;
	month: string;
	day: string;
	city: string;
	sport: string;
	event: string;
	url: string;
	medal: "Gold" | "Silver" | "Bronze";
	winner: string;
	country: string;
}

const processFile = async () => {
	const records: Row[] = [];
	const parser = fs
		.createReadStream("./data/olympic-medals.csv")
		.pipe(parse({ columns: true }));
	parser.on("readable", function () {
		let record;
		while ((record = parser.read()) !== null) {
			records.push(record);
		}
	});
	await finished(parser);
	return records;
};

const medalEmoji = {
	Gold: "🥇",
	Silver: "🥈",
	Bronze: "🥉",
};

function renderRow(row: Row) {
	try {
		const winner = row.winner.includes(row.country)
			? row.winner
			: `${row.winner} (${row.country})`;
		return `${medalEmoji[row.medal]}: ${winner}`;
	} catch (ex) {
		console.log(`row: ${row}`);
		throw ex;
	}
}

export function buildText(record: Row, records: Row[]) {
	const { season, year, month, day, sport, event, city } = record;
	const medals = _.filter(records, { season, year, month, day, sport, event });
	const topLine = `${city} ${year} - ${season} Olympics\n${sport} - ${event}`;
	const { Gold, Silver, Bronze } = _.groupBy(medals, "medal");
	const medalStrs = _.chain(_.concat(Gold || [], Silver || [], Bronze || []))
		.map(renderRow)
		.join("\n");
	return {
		text: `${topLine}\n\n${medalStrs}`,
		link: `https://www.olympedia.org${record.url}`,
	};
}

export async function testAll() {
	const records = await processFile();
	for (const record of records) {
		buildText(record, records);
	}
}

export async function getPostText() {
	const records = await processFile();
	const record = _.sample(records) as Row;
	const ret = buildText(record, records);
	const rt = new RichText({ text: `${ret.text}\n\n\n${ret.link}` });
	await rt.detectFacets(new AtpAgent({ service: "https://bsky.social" }));
	return { text: rt.text, facets: rt.facets as Facet[] };
}
