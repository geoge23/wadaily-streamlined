import conn, { Schedules } from './mongo';

export default async function getScheduleList(name) {
    await conn();
    const sch = await Schedules.findOne({name}).lean();
    //workaround ensures NextJS can dehydrate and rehydrate from server -> client for ISR
    if (sch) {
        sch._id = sch._id.toString();
        sch.schedule = sch.schedule.map(e => {
            e._id = e._id.toString();
            return e;
        })
    }
    return (sch) || {
        name: "NONE",
        friendlyName: "No School Day",
        schedule: []
    }
}