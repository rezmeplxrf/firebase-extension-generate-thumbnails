const path = require("path");
const os = require("os");
const fsPromises = require("fs").promises;
const { onObjectFinalized } = require("firebase-functions/v2/storage");
const { initializeApp } = require("firebase-admin/app");
const { getStorage } = require("firebase-admin/storage");
const ffmpeg = require("fluent-ffmpeg");

// try {
//     const ffmpegPath = require("@ffmpeg-installer/ffmpeg").path;
//     ffmpeg.setFfmpegPath(ffmpegPath);
// } catch (error) {
//     console.warn("Could not set ffmpeg path from installer, using system default");
// }


initializeApp({
    storageBucket: "roomi-9d2cd",
});



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

exports.processVideos = onObjectFinalized({
    region: "asia-northeast3",
    cpu: 1,
    storageBucket: "roomi-9d2cd",
    memory: "4GiB",
    maxInstances: 100,
    concurrency: 100,
    timeoutSeconds: 540,
    eventarc: {
        eventFilters: {
            "object": "media/videos/**"
        }
    }
},
    async (event) => {
        const object = event.data;
        const bucket = getStorage("roomi-9d2cd").bucket(object.bucket);
        const filePath = object.name;
        const contentType = object.contentType || "";
        const dir = path.dirname(filePath);
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

            const thumbfileName = removeFileExtension(fileName) + "." + process.env.IMAGE_TYPE;

            localThumbFilePath = path.join(os.tmpdir(), thumbfileName);

            const cloudThumbFilePath = path.join(
                getThumbnailPath(undefined, dir),
                thumbfileName,
            );

            // Prepare conversion paths if needed
            let mp4FileName, cloudMp4FilePath;
            if (!isAlreadyMp4) {
                mp4FileName = removeFileExtension(fileName) + ".mp4";
                localMp4FilePath = path.join(os.tmpdir(), mp4FileName);
                cloudMp4FilePath = path.join(dir, mp4FileName);
            }

            // Run thumbnail generation and video conversion in parallel
            const operations = [
                takeScreenshot(tempFilePath, thumbfileName)
            ];

            if (!isAlreadyMp4) {
                operations.push(convertToMp4(tempFilePath, localMp4FilePath));
            }

            await Promise.all(operations);

            // Verify files were created
            if (!(await fileExists(localThumbFilePath))) {
                throw new Error("Failed to locate generated thumbnail file");
            }

            if (!isAlreadyMp4 && !(await fileExists(localMp4FilePath))) {
                throw new Error("Failed to locate converted MP4 file");
            }

            // Upload files in parallel
            const uploadOperations = [
                bucket.upload(localThumbFilePath, {
                    destination: cloudThumbFilePath,
                    metadata: {
                        contentType: `image/${process.env.IMAGE_TYPE}`,
                        cacheControl: 'public, max-age=31536000',
                    },
                    public: true,
                })
            ];

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
            }

            await Promise.all(uploadOperations);

            // Delete original file if conversion was performed
            if (!isAlreadyMp4) {
                await bucket.file(filePath).delete();
            }

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
    return new Promise((resolve, reject) => {
        ffmpeg({ source: videoFilePath })
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
                    timestamps: [Number(process.env.TIMESTAMP)], // in seconds
                    filename: newFileName,
                },
                os.tmpdir(),
            )
            .withAspectRatio(process.env.ASPECT_RATIO);
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

        if (process.env.VIDEO_SIZE) {
            command = command.size(process.env.VIDEO_SIZE);
        }

        if (process.env.VIDEO_BITRATE) {
            command = command.videoBitrate(process.env.VIDEO_BITRATE);
        }

        if (process.env.AUDIO_BITRATE) {
            command = command.audioBitrate(process.env.AUDIO_BITRATE);
        }

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
 * Checks if directory matches the configured video path
 * @param {string} dir - Directory to check
 * @param {string} videoPath - Configured video path
 * @return {boolean} True if directory is valid
 */
function checkVideoDirectory(dir, videoPath) {
    const trimmedPath = videoPath && videoPath.replace(/^\/|\/$/g, "");
    const trimmedDir = dir && dir.replace(/^\/|\/$/g, "");

    if (
        videoPath === "~" ||
        (["", ".", "/"].includes(videoPath) && dir === ".") ||
        trimmedPath == trimmedDir
    ) {
        return true;
    } else return false;
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

/**
 * Gets the thumbnail path based on configuration
 * @param {string} pathString - Configured thumbnail path
 * @param {string} videoPath - Video directory path
 * @return {string} Resolved thumbnail path
 */
function getThumbnailPath(pathString, videoPath) {
    if (!pathString || pathString === "/") return "";
    if (pathString === "~") return videoPath + "/";
    return pathString;
}
