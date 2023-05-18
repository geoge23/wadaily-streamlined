import dbConnect from "../../../functions/mongo";
import { parseAndUpdateDays } from "../../../functions/parseCSV";

/**
 * API endpoint for uploading CSV day files. 
 * Takes a body of text/csv and parses it into the database.
 * 
 * @param {Request} req 
 * @param {Response} res 
 */
export default async function handler(req, res) {
    if (req.method !== 'POST') return res.status(405).send({error: "Method not allowed"})
    if (req.headers['content-type'] !== 'text/csv') return res.status(400).send({error: "Content type must be text/csv"})
    const { body } = req;
    if (!body) return res.status(400).send({error: "No body provided"})

    //checks bearer token against the UPLOAD_KEY env variable for authentication
    if (req.headers.authorization !== `Bearer ${process.env.UPLOAD_KEY}`) return res.status(401).send({error: "Unauthorized"})

    try {
        await dbConnect()
        await parseAndUpdateDays(body)
        return res.status(200).send({success: true})
    } catch (e) {
        console.error(e)
        return res.status(500).send({error: "Internal server error"})
    }
}