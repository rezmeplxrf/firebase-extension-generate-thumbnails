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
    cpu: 1,
    memory: "4GiB",
    maxInstances: 100,
    concurrency: 100,
    timeoutSeconds: 540,
    eventarc: {
        eventFilters: {
            "object": "media/temp/**"
        }
    }
},
    async (event) => {
        const object = event.data;
        const bucket = getStorage().bucket();
        const filePath = object.name;
        const contentType = object.contentType || "";
        const fileName = path.basename(filePath);
        const fileExtension = path.extname(fileName).toLowerCase();

        if (!contentType.includes("video/")) return;

        const isAlreadyMp4 = fileExtension === ".mp4";
        let tempFilePath, localThumbFilePath, localMp4FilePath;

        try {
            tempFilePath = path.join(os.tmpdir(), fileName);

            await bucket.file(filePath).download({ destination: tempFilePath });

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
            let mp4FileName, cloudMp4FilePath;
            if (!isAlreadyMp4) {
                mp4FileName = removeFileExtension(fileName) + ".mp4";
                localMp4FilePath = path.join(os.tmpdir(), mp4FileName);
                cloudMp4FilePath = path.join("media/videos", mp4FileName);
            } else {
                // For existing MP4 files, move them to media/videos/
                mp4FileName = fileName;
                cloudMp4FilePath = path.join("media/videos", mp4FileName);
            }

            // Run thumbnail generation and video conversion in parallel
            const operations = [];

            if (!isAlreadyProcessed) {
                operations.push(takeScreenshot(tempFilePath, thumbfileName));
            }

            if (!isAlreadyMp4) {
                operations.push(convertToMp4(tempFilePath, localMp4FilePath));
            }

            if (operations.length > 0) {
                await Promise.all(operations);
            }

            // Verify files were created
            if (!isAlreadyProcessed && !(await fileExists(localThumbFilePath))) {
                throw new Error("Failed to locate generated thumbnail file");
            }

            if (!isAlreadyMp4 && !(await fileExists(localMp4FilePath))) {
                throw new Error("Failed to locate converted MP4 file");
            }

            // Upload files in parallel
            const uploadOperations = [];

            if (!isAlreadyProcessed) {
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
            }

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
                    }).then(() => {
                        return bucket.file(cloudMp4FilePath).makePublic();
                    })
                );
            }

            if (uploadOperations.length > 0) {
                await Promise.all(uploadOperations);
            }

            // Always delete original file from temp directory
            await bucket.file(filePath).delete();

        } catch (error) {
            console.error("Error processing video:", error);
        } finally {
            // Always cleanup temporary files
            await Promise.allSettled([
                cleanupFile(tempFilePath),
                cleanupFile(localThumbFilePath),
                cleanupFile(localMp4FilePath)
            ]);
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
            .on("filenames", (filenames) => { })
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
        let command = ffmpeg(inputPath)
            .videoCodec("libx264")
            .audioCodec("aac");



        command
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