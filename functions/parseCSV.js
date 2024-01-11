import { parse } from 'csv-parse/sync';
import { Days, Schedules } from './mongo.js';

/** 
 * @typedef {{
 *  "Event Date": string,
 *  "Event Caption": string,
 *  "Event Description": string
 * }} CSVDay
 */

/**
 * @typedef {{
 * "Description": string,
 * "End Time": string,
 * "Start Time": string,
 * "Block Schedule": string,
 * "Day": string
 * }} CSVEvent
 */

/**
 * Parses a date string of the format:
 * MM/DD/YY
 * and returns a Date object.
 * 
 * @param {string} date
 * @returns {Date}
 */
function parseVeracrossDate(date) {
    const dateArray = date.split('/').map(e => parseInt(e));
    return new Date(dateArray[2] + 2000, dateArray[0] - 1, dateArray[1]);
}

/**
 * Generates a date string of the format:
 * M-D-YY
 * 
 * @param {Date} date
 * @returns {string}
 */
function generateWAFormatDate(date) {
    return `${date.getMonth() + 1}-${date.getDate()}-${date.getFullYear() % 100}`
}

/**
 * Parses a CSV file of the format:
 * Event Date,Event Caption,Event Description
 * and uploads it to the database.
 * 
 * @param {string} csv 
 */
async function parseAndUpdateDays(csv) {
    console.log("Parsing and uploading days...")

    /**
     * @type {CSVDay[]}
     */
    const days = parse(csv, {
        columns: true,
        skip_empty_lines: true
    })

    const promises = [];
    for (const day of days) {
        // Skip semester start and end days, which are present in the calendar
        if (day['Event Caption'].includes('Semester')) continue;

        const date = parseVeracrossDate(day['Event Date']);
        const waDate = generateWAFormatDate(date);

        // Event captions may come in the format:
        // "US Day 2", "US Day 3", etc.
        // in which case they are handled normally
        // or they may come in the format:
        // "US Day 2 (X Day - US)", "US Day 2 (X Day - US)", etc.
        // in which case they are handled by only taking the part in
        // parentheses.

        const regex = /\((.*)\)/;
        const match = regex.exec(day['Event Caption']);
        if (match) {
            day['Event Caption'] = match[1];
        }

        const dayDocument = new Days({
            schedule: day['Event Caption'],
            date: waDate
        })

        promises.push(
            {
                promise: dayDocument.save()
                    .catch(e => {
                        // if the day already exists, we can just update it
                        if (e.code == 11000) {
                            promises.push({
                                promise: Days.findOneAndUpdate({ date: waDate }, { schedule: day['Event Caption'] }),
                                date: waDate + " (update)"
                            })
                            console.log()
                        } else {
                            throw e;
                        }
                    }),
                date: waDate
            }
        )
    }

    let initialLength = promises.length;
    while (promises.length > 0) {
        process.stdout.clearLine(0);
        process.stdout.cursorTo(0);
        process.stdout.write(`${initialLength - promises.length + 1} / ${initialLength} days saved - Working on ${promises[promises.length - 1].date}`);
        await promises.pop().promise;
        await new Promise(resolve => setTimeout(resolve, 5));
    }
    process.stdout.clearLine(0);
    process.stdout.cursorTo(0);
    process.stdout.write(`All ${initialLength} days saved\n`)
}

/**
 * Takes a CSVEvent object and generates a schedule identifier
 * that lines up with those used in the Days csv.
 * 
 * @param {CSVEvent} event
 * @returns {string}
 */
function generateScheduleIdentifier(event) {
    // a "-" in the Day column indicates that the schedule
    // is a "Day 1" style schedule, as opposed to 
    // the X, A, B schedules
    if (event['Day'] != "-") {
        //extracts the number from a string like "US D4"
        const regex = /(\d+)/g;
        const match = regex.exec(event['Day']);
        if (!match) {
            return "ORPHAN"
        }
        return `US Day ${match[0]}`
    }

    // otherwise, just use the Block Schedule column
    return event['Block Schedule']
}

/**
 * Generates a schedule event object from a CSVEvent object.
 * 
 * @param {CSVEvent} event
 * @returns {{
 *  name: string,
 *  code: string,
 *  startTime: string,
 *  endTime: string
 * }}
 */
function generateScheduleEvent(event) {
    // if the title is in the form "Block C - US", we 
    // can extract the block letter "C" and use that as the code
    const regex = /Block ([A-Z])/;
    const match = regex.exec(event['Description']);
    const code = match ? match[1] : event['Description']

    //the start and end times should have capital AM/PM
    const startTime = event['Start Time'].toUpperCase()
    const endTime = event['End Time'].toUpperCase()

    // the name is the description, but we should
    // filter out the " - US" part if it exists
    const name = event['Description'].replace(/ - US/, '')

    return {
        name,
        code,
        startTime,
        endTime
    }
}


/**
 * Parses a CSV file of the format:
 * Description,End Time,Start Time,Block Schedule,Day
 * into distinct schedules using the "Block Schedule" column.
 * The schedules are then uploaded to the database.
 * 
 * @param {string} csv
 * @returns {Promise<CSVEvent[]>} A list of events that could not be parsed into a schedule
 */
async function parseAndUpdateSchedules(csv) {
    console.log("Parsing and uploading schedules...")

    /**
     * @type {Schedules[]}
     */
    const schedules = {}

    /**
     * @type {CSVEvent[]}
     */
    const events = parse(csv, {
        columns: true,
        skip_empty_lines: true
    })

    for (const event of events) {
        const identifier = generateScheduleIdentifier(event)

        // if the schedule already exists in this session, 
        // we can add the event to it
        if (schedules[identifier]) {
            schedules[identifier].schedule.push(generateScheduleEvent(event))
        } else {
            // otherwise, we need to create a new schedule

            // if the identifier contains a " - US", we can
            // remove it to get the friendly name
            const friendlyName = identifier.replace(/ - US/, '')

            schedules[identifier] = {
                name: identifier,
                friendlyName,
                schedule: [generateScheduleEvent(event)]
            }
        }
    }

    const promises = [];
    for (const schedule of Object.values(schedules)) {
        // sort the schedule by start time so that it is in order
        // time is in the format "HH:MM AM/PM"
        schedule.schedule.sort((a, b) => {
            const aTime = a.startTime.split(' ');
            const bTime = b.startTime.split(' ');

            let aHour = parseInt(aTime[0].split(':')[0]);
            if (aTime[1] == 'PM' && aHour != 12) aHour += 12;

            let bHour = parseInt(bTime[0].split(':')[0]);
            if (bTime[1] == 'PM' && bHour != 12) bHour += 12;

            const aMinute = parseInt(aTime[0].split(':')[1]);
            const bMinute = parseInt(bTime[0].split(':')[1]);

            if (aHour != bHour) return aHour - bHour;
            return aMinute - bMinute;
        })

        const scheduleDocument = new Schedules(schedule)
        promises.push(
            {
                promise: scheduleDocument.save()
                    .catch(e => {
                        // if the schedule already exists, we can just update it
                        if (e.code == 11000) {
                            promises.push({
                                promise: Schedules.findOneAndUpdate({ name: schedule.name }, schedule),
                                name: schedule.name + " (update)"
                            })
                        } else {
                            throw e;
                        }
                    }),
                name: schedule.name
            }
        )
    }

    let initialLength = promises.length;
    while (promises.length > 0) {
        process.stdout.clearLine(0);
        process.stdout.cursorTo(0);
        process.stdout.write(`${initialLength - promises.length + 1} / ${initialLength} schedules saved - Working on ${promises[promises.length - 1].name}`);
        await promises.pop().promise;
        await new Promise(resolve => setTimeout(resolve, 5));
    }

    process.stdout.clearLine(0);
    process.stdout.cursorTo(0);
    process.stdout.write(`All ${initialLength} schedules saved\n`)

    if (schedules['ORPHAN']) return schedules['ORPHAN'].schedule

    return []
}

export {
    parseAndUpdateDays,
    parseAndUpdateSchedules
};
