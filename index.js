require('dotenv').config();
const { Client } = require('pg');
const fs = require('fs');
const path = require('path');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');


const pgClient = new Client({
    host: process.env.PG_HOST,
    port: process.env.PG_PORT,
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD,
    database: process.env.PG_DATABASE,
});

const s3 = new S3Client({
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
    }
});

async function fetchBase64AndUpload() {
    try {
        await pgClient.connect();

        const query = `
            SELECT
                dd.id,
                dd.data,
                d.title
            FROM
                "DocumentData" dd
                    JOIN
                "Document" d ON d."documentDataId" = dd.id
            WHERE
                dd.type = 'BYTES_64'
        `;
        const res = await pgClient.query(query);

        for (const row of res.rows) {
            const { id, data, title } = row;

            if (!data) {
                console.warn(`No base64 data at ${id}`);
                continue;
            }

            const buffer = Buffer.from(data, 'base64');
            const filename = `${process.env.S3_FOLDER || 'documents'}/${id}_${title}.pdf`;

            await s3.send(new PutObjectCommand({
                Bucket: process.env.S3_BUCKET_NAME,
                Key: filename,
                Body: buffer,
                ContentType: 'application/pdf',
            }));

            console.log(`☁Uploaded to S3: ${filename}`);

            await pgClient.query(
                `UPDATE "DocumentData" SET data = $1, type = 'S3_PATH' WHERE id = $2`,
                [filename, id]
            );

            console.log(`Updated DB with filename for ID: ${id}`);
        }

    } catch (err) {
        console.error('Error:', err);
    } finally {
        await pgClient.end();
    }
}

fetchBase64AndUpload();
