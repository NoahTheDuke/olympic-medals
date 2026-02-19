import fs from "node:fs";
import { parse } from "csv-parse";
import { finished } from "stream/promises";
import _ from "lodash";
import { countries } from "./countries.js";

// Olympiad,Discipline,Event,Winner,Medal_type,Olympic_city,Olympic_year,Olympic_season,Gender,Code,Committee,Committee_type
// Athina 1896,Artistic Gymnastics,"Horizontal Bar, Men",Hermann Weingärtner,Gold,Athens,1896,summer,Men,GER,Germany,Country

interface Row {
	olympiad: string;
	sport: string;
	event: string;
	winner: string;
	medal_type: "Gold" | "Silver" | "Bronze";
	city: string;
	year: string;
	season: string;
	gender: string;
	code: string;
	committee: string;
	committee_type: string;
}

const processFile = async () => {
	const records: Row[] = [];
	const parser = fs
		.createReadStream(`./data/olympic_medals.csv`)
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
	const country = countries[row.code];
	const winner = row.winner.includes(country)
		? `Team ${row.winner}`
		: `${row.winner} (${row.committee})`;
	return `${medalEmoji[row.medal_type]}: ${winner}`;
}

function buildText(records: Row[]): string | undefined {
	let retries = 0;
	while (retries < 5) {
		retries += 1;
		const { olympiad, sport, event, year, city, season } = _.sample(records) as Row;
		const medals = _.filter(records, (row) => {
			return olympiad == row.olympiad && sport == row.sport && event == row.event;
		});
		const topLine = `${city} ${year} - ${season}\n${sport} - ${event}`;
		const { Gold, Silver, Bronze } = _.groupBy(medals, "medal_type");
		const medalStrs = _.chain(_.concat(Gold, Silver, Bronze))
			.map(renderRow)
			.join("\n");
		const result = `${topLine}\n\n${medalStrs}`;
		if (result.length < 300) {
			return result;
		}
	}
}

export default async function getPostText() {
	const records = await processFile();
	return buildText(records);
}
