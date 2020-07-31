using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace change_file
{
    class FileChanger
    {
        string filePath;
        int loopIntervalMs = 0;
        bool exit = false;
        NLog.Logger logger;

        delegate void DoChangeFileMethod();
        DoChangeFileMethod ChangeFile;

        public NLog.Logger Logger
        {
            set
            {
                logger = value;
            }
        }

        public string FilePath
        {
            set
            {
                filePath = value;
            }
        }

        public int LoopIntervalMs
        {
            set
            {
                loopIntervalMs = value;
            }
        }

        public void SetMode(int mode)
        {
            ChangeFile = LoopTouchFile;
            if (mode == 1)
            {
                ChangeFile = LoopTouchFile;
            }
            else if (mode == 2)
            {
                ChangeFile = LoopFlushFile;
            }
            else if (mode == 3)
            {
                ChangeFile = LoopWriteFile;
            }
        }

        public void Close()
        {
            exit = true;
        }

        public void Start()
        {
            if (!File.Exists(filePath))
            {
                throw new Exception(string.Format("file \"{0}\" note exist", filePath));
            }
            logger.Info("[FileToucher.LoopFlushFile] file path: \"{0}\"", filePath);

            Task.Run(()=> { ChangeFile(); });
        }

        void LoopFlushFile()
        {
            try
            {
                logger.Info("[FileToucher.LoopFlushFile] enter");
                using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
                {
                    List<byte> s = new List<byte>();
                    s.Add(65);
                   
                    while (true)
                    {
                        if (exit)
                        {
                            break;
                        }
                        Thread.Sleep(loopIntervalMs);
                        fs.Seek(0, SeekOrigin.Begin);
                        
                        fs.Write(s.ToArray(), 0, 1);
                        fs.Flush();
                        logger.Info("[FileToucher.LoopFlushFile] file fushed");

                        s[0]++;
                        if (s[0] > 65 + 25)
                        {
                            s[0] = 65;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                logger.Error("[FileToucher.LoopFlushFile] exception: \"{0}\", stackTrace[{1}]", e.Message, e.StackTrace);
            }
            finally
            {
                logger.Info("[FileToucher.LoopFlushFile] exit");
            }
        }

        void LoopWriteFile()
        {
            try
            {
                logger.Info("[FileToucher.LoopFlushFile] enter");
                List<byte> s = new List<byte>();
                s.Add(65);

                while (true)
                {
                    if (exit)
                    {
                        break;
                    }
                    Thread.Sleep(loopIntervalMs);

                    using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
                    {
                        fs.Seek(0, SeekOrigin.Begin);

                        fs.Write(s.ToArray(), 0, 1);
                        fs.Flush();
                    }
                    logger.Info("[FileToucher.LoopFlushFile] file writed");

                    s[0]++;
                    if (s[0] > 65 + 25)
                    {
                        s[0] = 65;
                    }
                }
            }
            catch (Exception e)
            {
                logger.Error("[FileToucher.LoopFlushFile] exception: \"{0}\", stackTrace[{1}]", e.Message, e.StackTrace);
            }
            finally
            {
                logger.Info("[FileToucher.LoopFlushFile] exit");
            }
        }

        void LoopTouchFile()
        {
            try
            {
                logger.Info("[FileToucher.LoopChangeFile] enter");
                while (true)
                {
                    if (exit)
                    {
                        break;
                    }
                    Thread.Sleep(loopIntervalMs);

                    DateTime touchTime = DateTime.Now;
                    File.SetLastWriteTime(filePath, touchTime);
                    logger.Info("[FileToucher.LoopChangeFile] touchTime {0}", touchTime.ToString("yyyy-MM-dd HH:mm:ss.ffff"));
                }
            }
            catch (Exception e)
            {
                logger.Error("[FileToucher.LoopChangeFile] exception: \"{0}\", stackTrace[{1}]", e.Message, e.StackTrace);
            }
            finally
            {
                logger.Info("[FileToucher.LoopChangeFile] exit");
            }
        }
    }
}
