import fs from "node:fs";
import { parse } from "csv-parse";
import { finished } from "stream/promises";
import _ from "lodash";
import { AtpAgent, Facet, FacetLink, RichText } from "@atproto/api";
import { isLink } from "@atproto/api/dist/client/types/app/bsky/richtext/facet.js";

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
		title: event,
		uri: `https://www.olympedia.org${record.url}`,
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
	const post = {
		text: ret.text,
		embed: {
			$type: "app.bsky.embed.external",
			external: {
				uri: ret.uri,
				title: `Olympedia - ${ret.title}`,
				description: "",
			},
		},
	};
	return post;
}
