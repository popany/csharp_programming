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
            return string.Format("serialNumber: {0}, fileWriteTime: {1}, occredTime: {2}", serialNumber, fileWriteTime.ToString("yyyy-MM-dd HH:mm:ss.ffff"), occuredTime.ToString("yyyy-MM-dd HH:mm:ss.ffff"));
        }
    }

    public class FileCopiedInfo
    {
        public FileChangeInfo fileChangeInfo;
        public string copiedFilePath;
        public DateTime copiedCompleteTime;

        public override string ToString()
        {
            return string.Format("fileChangeInfo: [{0}], copiedFilePath: {1}, copiedCompleteTime: {2}", fileChangeInfo, copiedFilePath, copiedCompleteTime.ToString("yyyy-MM-dd HH:mm:ss.ffff"));
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
            logger.Info("[FileToucher.LoopFlushFile] source file path: \"{0}\"", sourceFilePath);
            logger.Info("[FileToucher.LoopFlushFile] target file path: \"{0}\"", targetFilePath);
            Task.Run(() => CopyOnFileChange());
            Task.Run(() => ProcessCopiedFile());
        }

        void OnChanged(object source, FileSystemEventArgs e)
        {
            lock(fileChangeInfo)
            {
                fileChangeInfo.fileWriteTime = File.GetLastWriteTime(sourceFilePath);
                fileChangeInfo.occuredTime = DateTime.Now;
                fileChangeInfo.serialNumber++;
            }
            fileChanged.Set();
        }

        void StartFileWatcher()
        {
            fileSystemWatcher.Path = Path.GetDirectoryName(sourceFilePath);
            fileSystemWatcher.Filter = Path.GetFileName(sourceFilePath);
            fileSystemWatcher.NotifyFilter = NotifyFilters.LastWrite;
            fileSystemWatcher.Changed += OnChanged;
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

                    fileCopiedInfo.copiedFilePath = targetFilePath + DateTime.Now.ToString("_yyyyMMdd-HHmmss.ffff");
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
                            logger.Warn("[OnFileChangeCopier.ProcessCopiedFile] file changed on copy, before({0}), after({1})", fileWriteTime.ToString("yyyy-MM-dd HH:mm:ss.ffff"), fileChangeInfo.fileWriteTime.ToString("yyyy-MM-dd HH:mm:ss.ffff"));
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

