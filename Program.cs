using CommandLine;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LogToTcp
{
    class Program
    {
        static void Main(string[] args)
        {
            var options = new LogToTcpCommandLineArguments();
            if (Parser.Default.ParseArguments(args, options))
            {
                // 87608
                Parallel.Invoke(() => { PipeFile(options.LogFile, options.Address, options.Port, options.Skip, options.Take); });

                while (progress < 1)
                {
                    Console.WriteLine("{0:P1} completed ({1}/{2})", progress, pos, len);
                    Thread.Sleep(1000);
                }
            }
        }

        private static double progress = 0;
        private static long pos;
        private static long len;

        private static async void PipeFile(string filename, string hostname, int port, long skip, long take)
        {
            var client = new TcpClient();

            await client.ConnectAsync(hostname, port);

            var outstream = client.GetStream();
            var written = 0;

            using (var fs = new FileStream(filename, FileMode.Open, FileAccess.Read))
            {
                using (var sr = new StreamReader(fs))
                {
                    var sw = new StreamWriter(outstream);
                    string line;
                    long linenumber = 0;
                    while (!sr.EndOfStream)
                    {
                        line = await sr.ReadLineAsync();
                        pos = fs.Position;
                        len = fs.Length;
                        if (linenumber > skip)
                        {
                            await sw.WriteLineAsync(line);
                            written++;
                        }
                        if (take > 0)
                        {
                            if (linenumber > skip)
                                progress = (double)(linenumber - skip) / (double)take;
                            if (linenumber > skip + take)
                            {
                                progress = 1;
                                await outstream.FlushAsync();
                                break;
                            }
                        }
                        else
                        {
                            progress = (double)fs.Position / (double)fs.Length;
                        }
                        linenumber++;
                        if (written % 1000 == 999)
                        {
                            await outstream.FlushAsync();
                            Thread.Sleep(500);
                        }
                    }
                }
            }

            client.Close();
        }

        
    }

    public class LogToTcpCommandLineArguments
    {
        [Option('f', "logFile", Required = true, HelpText = "")]
        public string LogFile { get; set; }

        [Option('s', "skip", Required = false, HelpText = "Numbers of lines to skip", DefaultValue = 0)]
        public int Skip { get; set; }

        [Option('t', "take", Required = false, HelpText = "Numbers of lines to take", DefaultValue = 0)]
        public int Take { get; set; }

        [Option('a', "address", Required = true, HelpText = "Address to send data to")]
        public string Address { get; set; }

        [Option('p', "port", Required = true, HelpText = "Port number to send data to")]
        public int Port { get; set; }
    }
}
