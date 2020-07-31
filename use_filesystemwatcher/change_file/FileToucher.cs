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
    class FileToucher
    {
        string filePath;
        int loopIntervalMs = 0;
        bool exit = false;
        NLog.Logger logger;

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

            Task.Run(()=> { LoopTouchFile(); });
        }

        void LoopTouchFile()
        {
            try
            {
                logger.Info("[FileToucher.LoopTouchFile] enter");
                while (true)
                {
                    Thread.Sleep(loopIntervalMs);

                    DateTime touchTime = DateTime.Now;
                    File.SetLastWriteTime(filePath, touchTime);
                    logger.Info("[FileToucher.LoopTouchFile] touchTime {0}", touchTime.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                }
            }
            catch (Exception e)
            {
                logger.Error("[FileToucher.LoopTouchFile] exception: \"{0}\", stackTrace[{1}]", e.Message, e.StackTrace);
            }
            finally
            {
                logger.Info("[FileToucher.LoopTouchFile] exit");

            }
        }
    }
}
