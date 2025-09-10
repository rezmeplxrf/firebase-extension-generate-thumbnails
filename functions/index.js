const path = require("path");
const os = require("os");
const fsPromises = require("fs").promises;
const { onObjectFinalized } = require("firebase-functions/v2/storage");
const { initializeApp } = require("firebase-admin/app");
const { getStorage } = require("firebase-admin/storage");
const ffmpeg = require("fluent-ffmpeg");




initializeApp();


exports.processVideos = onObjectFinalized({
    region: "asia-northeast3",
    cpu: 2,
    memory: "4GiB",
    maxInstances: 100,
    concurrency: 100,
    timeoutSeconds: 540
},
    async (event) => {
        const object = event.data;
        // skip if it's not /media/temp/
        console.log("File uploaded:", object.name);
        if (!object.name.startsWith("media/temp/")) {
            console.log("File is not in media/temp/, skipping..");
            return null;
        }


        const bucket = getStorage().bucket();
        const filePath = object.name;
        const contentType = object.contentType || "";
        const fileName = path.basename(filePath);
        console.log(`Processing file: ${fileName}, contentType: ${contentType}`);
        const fileExtension = path.extname(fileName).toLowerCase();

        if (!contentType.includes("video/")) return;


        let tempFilePath, localThumbFilePath, localMp4FilePath, isAlreadyMp4;

        try {
            tempFilePath = path.join(os.tmpdir(), fileName);

            // Download file using stream to reduce memory usage
            const downloadStream = bucket.file(filePath).createReadStream();
            const writeStream = require('fs').createWriteStream(tempFilePath);

            await new Promise((resolve, reject) => {
                downloadStream
                    .pipe(writeStream)
                    .on('error', reject)
                    .on('finish', resolve);
            });

            if (!(await fileExists(tempFilePath))) {
                throw new Error("Could not locate downloaded file");
            }

            const thumbfileName = removeFileExtension(fileName) + "." + "webp";

            localThumbFilePath = path.join(os.tmpdir(), thumbfileName);

            const cloudThumbFilePath = path.join(
                "media/thumbnails",
                thumbfileName,
            );

            // Prepare conversion paths if needed
            // 일단 모든 파일을 인코딩
            isAlreadyMp4 = false; // fileExtension === ".mp4";
            const baseName = removeFileExtension(fileName);
            const mp4FileName = baseName + ".mp4";
            const cloudMp4FilePath = path.join("media/videos", mp4FileName);

            if (tempFilePath === path.join(os.tmpdir(), mp4FileName)) {
                // Input is already an MP4, create a new name for the output to avoid conflict
                localMp4FilePath = path.join(os.tmpdir(), baseName + "_converted.mp4");
            } else {
                localMp4FilePath = path.join(os.tmpdir(), mp4FileName);
            }
            console.log(`localMp4FilePath: ${localMp4FilePath}, cloudMp4FilePath: ${cloudMp4FilePath}`);

            // Run thumbnail generation and video conversion in parallel
            const operations = [];

            operations.push(takeScreenshot(tempFilePath, thumbfileName));


            operations.push(convertToMp4(tempFilePath, localMp4FilePath));


            // Wait for all processing operations to complete
            console.log("Waiting for operations to complete:", operations.length, "operations");
            await Promise.all(operations);
            console.log("Operations completed.");

            // Verify processed files exist before uploading
            if (!(await fileExists(localThumbFilePath))) {
                throw new Error(`Thumbnail generation failed - ${localThumbFilePath} not found`);
            }

            if (!isAlreadyMp4 && !(await fileExists(localMp4FilePath))) {
                throw new Error(`Video conversion failed - ${localMp4FilePath} not found`);
            }


            // Run uploads in parallel
            const uploadOperations = [];

            uploadOperations.push(
                bucket.upload(localThumbFilePath, {
                    destination: cloudThumbFilePath,
                    metadata: {
                        contentType: `image/webp`,
                        cacheControl: 'public, max-age=31536000',
                    },
                    public: true,
                })
            );

            if (!isAlreadyMp4) {
                uploadOperations.push(
                    bucket.upload(localMp4FilePath, {
                        destination: cloudMp4FilePath,
                        metadata: {
                            contentType: "video/mp4",
                            cacheControl: 'public, max-age=31536000',
                        },
                        public: true,
                    })
                );
            } else {
                // Move existing MP4 file to media/videos/ with proper metadata
                uploadOperations.push(
                    bucket.file(filePath).copy(bucket.file(cloudMp4FilePath), {
                        metadata: {
                            contentType: "video/mp4",
                            cacheControl: 'public, max-age=31536000',
                        }
                    }).then(() => bucket.file(cloudMp4FilePath).makePublic())
                );
            }

            // Wait for all uploads to complete
            await Promise.all(uploadOperations);


            // Always delete original file from temp directory
            await bucket.file(filePath).delete();

        } catch (error) {
            console.error("Error processing video:", error);
        } finally {
            // Cleanup temporary files that were actually created
            const cleanupPromises = [
                cleanupFile(tempFilePath),
                cleanupFile(localThumbFilePath)
            ];

            // Only cleanup MP4 file if it was actually created (not for existing MP4s)
            if (!isAlreadyMp4 && localMp4FilePath) {
                cleanupPromises.push(cleanupFile(localMp4FilePath));
            }

            await Promise.allSettled(cleanupPromises);
        }

        return null;
    });

/**
 * Takes a screenshot from a video file
 * @param {string} videoFilePath - Path to the video file
 * @param {string} newFileName - Name for the screenshot file
 * @return {Promise} Promise that resolves when screenshot is taken
 */
async function takeScreenshot(videoFilePath, newFileName) {
    return new Promise(async (resolve, reject) => {
        const aspectRatio = await getAspectRatio(videoFilePath);

        const command = ffmpeg({ source: videoFilePath });

        if (aspectRatio) {
            command.withAspectRatio(aspectRatio);
        }

        command
            .on("end", () => {
                resolve(null);
            })
            .on("error", (error) => {
                console.error(error);
                reject(error);
            })
            .takeScreenshots(
                {
                    count: 1,
                    timestamps: [Number(1)], // in seconds
                    filename: newFileName,
                },
                os.tmpdir(),
            );
    });
}

function getAspectRatio(videoFilePath) {
    return new Promise((resolve, reject) => {
        ffmpeg.ffprobe(videoFilePath, (err, metadata) => {
            if (err) {
                console.warn(`Could not get video metadata for ${videoFilePath}:`, err);
                resolve(null);
                return;
            }
            const videoStream = metadata.streams.find(stream => stream.codec_type === 'video');
            if (videoStream && videoStream.width && videoStream.height) {
                resolve(`${videoStream.width}:${videoStream.height}`);
            } else if (videoStream && videoStream.display_aspect_ratio) {
                resolve(videoStream.display_aspect_ratio);
            }
            else {
                resolve(null);
            }
        });
    });
}

/**
 * Converts a video file to MP4 format
 * @param {string} inputPath - Input video file path
 * @param {string} outputPath - Output MP4 file path
 * @return {Promise} Promise that resolves when conversion is complete
 */
async function convertToMp4(inputPath, outputPath) {
    return new Promise((resolve, reject) => {
        ffmpeg(inputPath)
            .videoCodec("libx264")
            .audioCodec("aac")
            .outputOptions([
                "-crf 28",
                "-preset veryfast",
                "-movflags +faststart"
            ])
            .on("end", () => {
                resolve(null);
            })
            .on("error", (error) => {
                console.error("Video conversion error:", error);
                reject(error);
            })
            .save(outputPath);
    });
}


/**
 * Removes file extension from filename
 * @param {string} filename - Filename with extension
 * @return {string} Filename without extension
 */
function removeFileExtension(filename) {
    const lastDotIndex = filename.lastIndexOf(".");
    const extensionNotFound = lastDotIndex === -1;
    return extensionNotFound ? filename : filename.substring(0, lastDotIndex);
}




// Helper function for safe file cleanup
async function cleanupFile(filePath) {
    try {
        if (filePath && await fileExists(filePath)) {
            await fsPromises.unlink(filePath);
        }
    } catch (error) {
        console.warn(`Failed to cleanup file ${filePath}:`, error.message);
    }
}

// Helper function to check if file exists asynchronously
async function fileExists(filePath) {
    try {
        await fsPromises.access(filePath);
        return true;
    } catch {
        return false;
    }
}