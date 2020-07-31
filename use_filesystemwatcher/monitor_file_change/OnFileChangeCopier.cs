using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;

namespace monitor_file_change
{
    public class FileChangeInfo
    {
        public Int64 serialNumber = 0;
        public DateTime fileWriteTime = DateTime.MinValue;
        public DateTime occuredTime = DateTime.MinValue;

        public override string ToString()
        {
            return string.Format("serialNumber: {0}, fileWriteTime: {1}, occredTime: {2}", serialNumber, fileWriteTime.ToString("yyyy-MM-dd HH:mm:ss.fff"), occuredTime.ToString("yyyy-MM-dd HH:mm:ss.fff"));
        }
    }

    public class FileCopiedInfo
    {
        public FileChangeInfo fileChangeInfo;
        public string copiedFilePath;
        public DateTime copiedCompleteTime;

        public override string ToString()
        {
            return string.Format("fileChangeInfo: [{0}], copiedFilePath: {1}, copiedCompleteTime: {2}", fileChangeInfo, copiedFilePath, copiedCompleteTime.ToString("yyyy-MM-dd HH:mm:ss.fff"));
        }
    }

    public class OnFileChangeCopier
    {
        private AutoResetEvent fileChanged = new AutoResetEvent(false);
        private AutoResetEvent fileCopied = new AutoResetEvent(false);
        NLog.Logger logger;
        FileChangeInfo fileChangeInfo = new FileChangeInfo();
        bool exit = false;
        string targetFilePath;
        string sourceFilePath;
        ConcurrentQueue<FileCopiedInfo> fileCopiedQueue = new ConcurrentQueue<FileCopiedInfo>();
        FileSystemWatcher fileSystemWatcher = new FileSystemWatcher();

        const int fileCopiedTimeOutMs = 3000;

        public NLog.Logger Logger
        {
            set
            {
                logger = value;
            }
        }

        public String TargetFilePath
        {
            set
            {
                targetFilePath = value;
            }
        }

        public String SourceFilePath
        {
            set
            {
                sourceFilePath = value;
            }
        }

        public OnFileChangeCopier()
        {
        }

        public void Close()
        {
            exit = true;
        }

        public void Start()
        {
            if (!File.Exists(sourceFilePath))
            {
                throw new Exception(string.Format("sourceFile \"{0}\" not exist", sourceFilePath));
            }
            Task.Run(() => CopyOnFileChange());
            Task.Run(() => ProcessCopiedFile());
        }

        void StartFileWatcher()
        {
            fileSystemWatcher.Path = Path.GetDirectoryName(sourceFilePath);
            fileSystemWatcher.Filter = Path.GetFileName(sourceFilePath);
            fileSystemWatcher.NotifyFilter = NotifyFilters.LastWrite;
            fileSystemWatcher.Changed += ((object source, FileSystemEventArgs e) => {
                lock(fileChangeInfo)
                {
                    fileChangeInfo.fileWriteTime = File.GetLastWriteTime(sourceFilePath);
                    fileChangeInfo.occuredTime = DateTime.Now;
                    fileChangeInfo.serialNumber++;
                }
                fileChanged.Set();
            });
            fileSystemWatcher.EnableRaisingEvents = true;
        }

        void CopyOnFileChange()
        {
            try
            {
                logger.Info("[OnFileChangeCopier.CopyOnFileChange] enter");
                StartFileWatcher();

                FileCopiedInfo fileCopiedInfo = new FileCopiedInfo();
                while (true)
                {
                    fileChanged.WaitOne();
                    if (exit)
                    {
                        return;
                    }
                    lock (fileChangeInfo)
                    {
                        fileCopiedInfo.fileChangeInfo = fileChangeInfo;
                    }
                    logger.Info("[OnFileChangeCopier.CopyOnFileChange] file changed, fileChangeInfo({0})", fileChangeInfo.ToString());

                    fileCopiedInfo.copiedFilePath = targetFilePath + DateTime.Now.ToString("_yyyyMMdd-HHmmss.fff");
                    File.Copy(sourceFilePath, fileCopiedInfo.copiedFilePath);
                    fileCopiedInfo.copiedCompleteTime = DateTime.Now;
                    fileCopiedQueue.Enqueue(fileCopiedInfo);
                    fileCopied.Set();
                }
            }
            catch (Exception e)
            {
                logger.Error("[OnFileChangeCopier.CopyOnFileChange] exception: \"{0}\", stackTrace[{1}]", e.Message, e.StackTrace);
            }
            finally
            {
                logger.Info("[OnFileChangeCopier.CopyOnFileChange] exit");
            }
        }

        void ProcessCopiedFile()
        {
            try
            {
                logger.Info("[OnFileChangeCopier.ProcessCopiedFile] enter");

                while (true)
                {
                    bool signaled = fileCopied.WaitOne(fileCopiedTimeOutMs);

                    if (exit)
                    {
                        break;
                    }

                    if (!signaled)
                    {
                        logger.Warn("[OnFileChangeCopier.ProcessCopiedFile] timeout");
                    }

                    FileCopiedInfo fileCopiedInfo = null;
                    while (fileCopiedQueue.TryDequeue(out fileCopiedInfo))
                    {
                        logger.Info("[OnFileChangeCopier.ProcessCopiedFile] fileCopiedInfo({0})", fileCopiedInfo.ToString());
                        DateTime fileWriteTime = File.GetLastWriteTime(fileCopiedInfo.copiedFilePath);
                        if (fileWriteTime != fileCopiedInfo.fileChangeInfo.fileWriteTime)
                        {
                            logger.Warn("[OnFileChangeCopier.ProcessCopiedFile] file changed on copy, before({0}), after({1})", fileWriteTime.ToString("yyyy-MM-dd HH:mm:ss.fff"), fileChangeInfo.fileWriteTime.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                        }
                    }
                }
            }
            catch (Exception e)
            {
                logger.Error("[OnFileChangeCopier.ProcessCopiedFile] exception: \"{0}\", stackTrace[{1}]", e.Message, e.StackTrace);
            }
            finally
            {
                logger.Info("[OnFileChangeCopier.ProcessCopiedFile] exit");
            }
        }
    }
}

