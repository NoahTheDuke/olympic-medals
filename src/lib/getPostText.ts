import fs from "node:fs";
import { parse } from "csv-parse";
import { finished } from "stream/promises";
import _ from "lodash";

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
    const parser = fs.createReadStream(`./data/olympic_medals.csv`).pipe(
        parse({ columns: true })
    );
    parser.on("readable", function() {
        let record;
        while ((record = parser.read()) !== null) {
            records.push(record);
        }
    });
    await finished(parser);
    return records;
};

const medalEmoji = {
    "Gold": "🥇",
    "Silver": "🥈",
    "Bronze": "🥉",
}

function renderRow(row: Row) {
    return `${medalEmoji[row.medal_type]}: ${row.winner}`
}

export default async function getPostText() {
    const records = await processFile();
    const { olympiad, sport, event, year, city, season } = _.sample(records) as Row;
    const medals = _.filter(records, (row) =>
        olympiad == row.olympiad &&
        sport == row.sport &&
        event == row.event
    );
    const gold = _.filter(medals, (row) => row.medal_type === "Gold");
    const silver = _.filter(medals, (row) => row.medal_type === "Silver");
    const bronze = _.filter(medals, (row) => row.medal_type === "Bronze");

    const seasonStr = season === 'summer' ? "Summer" : "Winter";
    const topLine = `${city} ${year} ${seasonStr} Olympics\n${event}`;
    const medalStrs = _.chain(_.concat(gold, silver, bronze))
        .map(renderRow)
        .join("\n");
    return `${topLine}\n\n${medalStrs}`;
}
