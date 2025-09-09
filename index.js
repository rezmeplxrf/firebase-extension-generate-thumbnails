
const { initializeApp } = require("firebase-admin/app");
const { getStorage } = require("firebase-admin/storage");
const { storage } = require("firebase-functions/v1");
const path = require("path");
const os = require("os");
const fs = require("fs");
const ffmpegPath = require("@ffmpeg-installer/ffmpeg").path;
const ffmpeg = require("fluent-ffmpeg");
const { v4: uuidv4 } = require("uuid");
ffmpeg.setFfmpegPath(ffmpegPath);

initializeApp();

exports.processVideos = storage.object().onFinalize(async (object, context) => {
   const fileBucket = object.bucket;
   const filePath = object.name;
   const contentType = object.contentType || "";
   const dir = path.dirname(filePath);
   const fileName = path.basename(filePath);
   const fileExtension = path.extname(fileName).toLowerCase();

   if (!checkVideoDirectory(dir, process.env.VIDEO_PATH) || !contentType.includes("video/")) return;

   const isAlreadyMp4 = fileExtension === ".mp4";

   try {
      const bucket = getStorage().bucket(fileBucket);
      const tempFilePath = path.join(os.tmpdir(), fileName);

      await bucket.file(filePath).download({ destination: tempFilePath });

      if (!fs.existsSync(tempFilePath)) throw "Could not locate downloaded file";

      const prefix = process.env?.THUMBNAIL_PREFIX ? process.env.THUMBNAIL_PREFIX : "";
      const suffix = process.env?.THUMBNAIL_SUFFIX ? process.env.THUMBNAIL_SUFFIX : "";
      const thumbfileName =
         prefix + removeFileExtension(fileName) + suffix + "." + process.env.IMAGE_TYPE;

      const localThumbFilePath = path.join(os.tmpdir(), thumbfileName);

      const cloudThumbFilePath = path.join(
         getThumbnailPath(process.env.THUMBNAIL_PATH, dir),
         thumbfileName
      );

      await takeScreenshot(tempFilePath, thumbfileName);

      if (!fs.existsSync(localThumbFilePath)) throw "Failed to locate generated file";

      await bucket.upload(localThumbFilePath, {
         destination: cloudThumbFilePath,
         metadata: {
            contentType: `image/${process.env.IMAGE_TYPE}`,
            metadata: {
               firebaseStorageDownloadTokens: uuidv4()
            }
         },
         public: false
      });

      fs.unlinkSync(localThumbFilePath);

      if (!isAlreadyMp4) {
         const mp4FileName = removeFileExtension(fileName) + ".mp4";
         const localMp4FilePath = path.join(os.tmpdir(), mp4FileName);
         const cloudMp4FilePath = path.join(dir, mp4FileName);

         await convertToMp4(tempFilePath, localMp4FilePath);

         if (!fs.existsSync(localMp4FilePath)) throw "Failed to locate converted MP4 file";

         await bucket.upload(localMp4FilePath, {
            destination: cloudMp4FilePath,
            metadata: {
               contentType: "video/mp4",
               metadata: {
                  firebaseStorageDownloadTokens: uuidv4()
               }
            },
            public: false
         });

         fs.unlinkSync(localMp4FilePath);
         
         await bucket.file(filePath).delete();
      }

      fs.unlinkSync(tempFilePath);
   } catch (error) {
      console.error("Error processing video:", error);
   }

   return null;
});

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
               timestamps: [Number(process.env.TIMESTAMP)], //in seconds
               filename: newFileName
            },
            os.tmpdir()
         )
         .withAspectRatio(process.env.ASPECT_RATIO);
   });
}

async function convertToMp4(inputPath, outputPath) {
   return new Promise((resolve, reject) => {
      let command = ffmpeg(inputPath)
         .videoCodec('libx264')
         .audioCodec('aac');

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
         .on('end', () => {
            resolve(null);
         })
         .on('error', (error) => {
            console.error('Video conversion error:', error);
            reject(error);
         })
         .save(outputPath);
   });
}

function checkVideoDirectory(dir, videoPath) {
   const trimmedPath = videoPath?.replace(/^\/|\/$/g, "");
   const trimmedDir = dir?.replace(/^\/|\/$/g, "");

   if (
      videoPath === "~" ||
      (["", ".", "/"].includes(videoPath) && dir === ".") ||
      trimmedPath == trimmedDir
   ) {
      return true;
   } else return false;
}

function removeFileExtension(filename) {
   const lastDotIndex = filename.lastIndexOf(".");
   const extensionNotFound = lastDotIndex === -1;
   return extensionNotFound ? filename : filename.substring(0, lastDotIndex);
}

function getThumbnailPath(pathString, videoPath) {
   if (!pathString || pathString === "/") return "";
   if (pathString === "~") return videoPath + "/";
   return pathString;
}
