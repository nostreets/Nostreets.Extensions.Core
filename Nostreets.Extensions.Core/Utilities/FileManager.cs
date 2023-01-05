using Nostreets.Extensions.Extend.Basic;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Nostreets.Extensions.Utilities
{
    public class FileManager
    {
        public FileManager()
        {
            TargetedDirectory = Directory.GetCurrentDirectory();
            _latestInstance = this;
        }

        public FileManager(string directory)
        {
            if (!directory.DirectoryExists()) { throw new Exception("Directory Path is not valid..."); }

            TargetedDirectory = directory;
            _latestInstance = this;
        }

        public string TargetedDirectory { get; set; }
        public string LastFileAccessed { get; private set; }
        public static FileManager LatestInstance { get => _latestInstance; }

        private static FileManager _latestInstance = null;

        private void NewLog(string[] args = null)
        {
            LatestInstance.WriteToFile("LOG START AT " + DateTime.Now.Timestamp() + "\n");

            if (args != null && args.Length > 0)
                for (int i = 0; i < args.Length; i++)
                {
                    LatestInstance.WriteToFile($"{i} ARGUEMENT IS: {args[i]}\n");
                }
        }


        public async Task WriteToFileAsync(string fileName, string textToWrite)
        {
            fileName = fileName ?? $"{AppDomain.CurrentDomain.FriendlyName}_LOG_{DateTime.Now.Timestamp()}.txt";
            string filePath = (!TargetedDirectory[TargetedDirectory.Length - 1].Equals("\\")) ? TargetedDirectory + "\\" + fileName : TargetedDirectory + fileName;
            string[] splitText = textToWrite.Split(new[] { "\n" }, StringSplitOptions.None);


            if (!File.Exists(filePath))
                CreateFile(fileName);

            if (splitText != null && splitText.Length > 0)
            {
                foreach (string text in splitText)
                {
                    using (StreamWriter sw = new StreamWriter(filePath, true))
                    {
                        await sw.WriteLineAsync(text);
                    }
                }
            }

            LastFileAccessed = fileName;

        }


        #region Public Methods
        public void CreateFile(string fileName)
        {
            string filePath = (!TargetedDirectory[TargetedDirectory.Length - 1].Equals("\\")) ? TargetedDirectory + "\\" + fileName : TargetedDirectory + fileName;

            if (!File.Exists(filePath))
            {
                using (File.Create(filePath)) { };
            }

            if (!File.Exists(filePath)) { throw new Exception("File was not created..."); }
            LastFileAccessed = fileName;
            NewLog();

        }

        public void WriteToFile(string textToWrite)
        {
            WriteToFileAsync(LastFileAccessed, textToWrite);
        }

        public void CopyFile(string dirOfFileToCopy)
        {
            CopyFile(dirOfFileToCopy, LastFileAccessed);
        }

        public void CopyFile(string targetDir, string nameOfFileToCopy)
        {
            string sourcePath = (!TargetedDirectory[TargetedDirectory.Length - 1].Equals("\\")) ? TargetedDirectory + "\\" + nameOfFileToCopy : TargetedDirectory + nameOfFileToCopy;
            string targetPath = (!targetDir[targetDir.Length - 1].Equals("\\")) ? targetDir + "\\" + nameOfFileToCopy : targetDir + nameOfFileToCopy;

            if (File.Exists(TargetedDirectory))
            {
                // If file already exists in destination, delete it.
                if (File.Exists(sourcePath))
                {
                    File.Delete(sourcePath);
                }

                File.Copy(targetPath, sourcePath);
            }

            LastFileAccessed = nameOfFileToCopy;

        }

        public void DeleteFile()
        {
            DeleteFile(LastFileAccessed);
        }

        public void DeleteFile(string fileName)
        {
            string filePath = (!TargetedDirectory[TargetedDirectory.Length - 1].Equals("\\")) ? TargetedDirectory + "\\" + fileName : TargetedDirectory + fileName;

            if (File.Exists(TargetedDirectory))
            {
                File.Delete(TargetedDirectory);
            }

            LastFileAccessed = fileName;

        }
        #endregion
    }
}
