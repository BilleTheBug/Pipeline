using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PipelineBalanced
{
    class PipelineBalancedProgram
    {
        private static readonly int SHORTSTAGE = 100;
        private static readonly int LONGSTAGE = 200;
        private static readonly int ARRAYSIZE = 10;
        
        private static Stopwatch st = new Stopwatch();
        private static List<int> result = new List<int>();

        static void Main(string[] args)
        {
            var token = new CancellationToken();
            PipelineBalanced(token);
            PrintResult();
        }
        
        private static void PipelineBalanced(CancellationToken token)
        {
            using (CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(token))
            {
                var bufferSize = 10;
                var data = InitializeData(ARRAYSIZE);

                var stage1To2Buffer = new BlockingCollection<int>(bufferSize);
                var stage2ToMultiplexerBuffer1 = new BlockingCollection<int>(bufferSize);
                var stage2ToMultiplexerBuffer2 = new BlockingCollection<int>(bufferSize);
                var multiplexerToStage3Buffer = new BlockingCollection<int>(bufferSize);
                var stage3To4Buffer = new BlockingCollection<int>(bufferSize);

                var factory = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.None);

                st.Restart();
                var stage1 = factory.StartNew(() => Stage1(stage1To2Buffer, data, cts));
                var stage2Producer1 = factory.StartNew(() => Stage2(stage1To2Buffer, stage2ToMultiplexerBuffer1, cts));
                var stage2Producer2 = factory.StartNew(() => Stage2(stage1To2Buffer, stage2ToMultiplexerBuffer2, cts));
                var multiplexer = factory.StartNew(() => Multiplexer(stage2ToMultiplexerBuffer1, stage2ToMultiplexerBuffer2, multiplexerToStage3Buffer, cts));
                var stage3 = factory.StartNew(() => Stage3(multiplexerToStage3Buffer, stage3To4Buffer, cts));
                var stage4 = factory.StartNew(() => Stage4(stage3To4Buffer, cts));

                Task.WaitAll(stage1, stage2Producer1, stage2Producer2, multiplexer, stage3, stage4);
            }
        }

        private static void Stage1(BlockingCollection<int> output, int[] data, CancellationTokenSource cts)
        {
            try
            {
                var token = cts.Token;
                foreach (int number in data)
                {
                    if (token.IsCancellationRequested)
                        break;
                    Thread.Sleep(SHORTSTAGE);
                    output.Add(number, token);
                    Console.WriteLine("Stage 1 processed number {0}", number);
                }
            }
            catch (Exception e)
            {
                cts.Cancel();
                if (!(e is OperationCanceledException))
                {
                    Console.WriteLine("Unexpected exception occured in stage 1");
                    throw;
                }
                else
                    Console.WriteLine("Operation cancelled in stage 1");
            }
            finally
            {
                output.CompleteAdding();
                Console.WriteLine("Stage 1 stopped");
            }
        }

        private static void Stage2(BlockingCollection<int> input, BlockingCollection<int> output, CancellationTokenSource cts)
        {
            try
            {
                var token = cts.Token;
                foreach (int number in input.GetConsumingEnumerable())
                {
                    if (token.IsCancellationRequested)
                        break;
                    //if (number == 4) throw new OperationCanceledException();
                    Thread.Sleep(LONGSTAGE);
                    output.Add(number, token);
                    Console.WriteLine("    Stage 2 processed number {0}", number);
                }
            }
            catch (Exception e)
            {
                cts.Cancel();
                if (!(e is OperationCanceledException))
                {
                    Console.WriteLine("    Unexpected exception occured in stage 2");
                    throw;
                }
                else
                    Console.WriteLine("    Operation cancelled in stage 2");
            }
            finally
            {
                output.CompleteAdding();
                Console.WriteLine("Stage 2 stopped");
            }
        }

        private static void Multiplexer(BlockingCollection<int> input1, BlockingCollection<int> input2, BlockingCollection<int> output, CancellationTokenSource cts)
        {
            try
            {
                var token = cts.Token;
                var producers = new BlockingCollection<int>[2] { input1, input2 };
                var nextExpected = 0;
                int number = -1;
                var lookAhead = new HashSet<int>();
                while (!input1.IsCompleted || !input2.IsCompleted)
                {
                    if (token.IsCancellationRequested)
                        break;
                    
                    if (BlockingCollection<int>.TryTakeFromAny(producers, out number) != -1 && number == nextExpected)
                    {
                        output.Add(number);
                        Console.WriteLine("Multiplexer added number {0}", number);
                        nextExpected++;
                    }
                    else if (lookAhead.Contains(nextExpected))
                    {
                        lookAhead.Remove(nextExpected);
                        output.Add(nextExpected);
                        nextExpected++;
                    }
                    else
                        lookAhead.Add(number);
                }
            }
            catch (Exception e)
            {
                cts.Cancel();
                if (!(e is OperationCanceledException))
                {
                    Console.WriteLine("Unexpected exception occured in multiplexer");
                    throw;
                }
                else
                    Console.WriteLine("Operation cancelled in multiplexer");
            }
            finally
            {
                output.CompleteAdding();
                Console.WriteLine("multiplexer stopped");
            }
        }

        private static void Stage3(BlockingCollection<int> input, BlockingCollection<int> output, CancellationTokenSource cts)
        {
            try
            {
                var token = cts.Token;
                foreach (int number in input.GetConsumingEnumerable())
                {
                    if (token.IsCancellationRequested)
                        break;
                    Thread.Sleep(SHORTSTAGE);
                    output.Add(number, token);
                    Console.WriteLine("        Stage 3 processed number {0}", number);
                }
            }
            catch (Exception e)
            {
                cts.Cancel();
                if (!(e is OperationCanceledException))
                {
                    Console.WriteLine("        Unexpected exception occured in stage 3");
                    throw;
                }
                else
                    Console.WriteLine("        Operation cancelled in stage 3");
            }
            finally
            {
                output.CompleteAdding();
                Console.WriteLine("Stage 3 stopped");
            }
        }
        private static void Stage4(BlockingCollection<int> input, CancellationTokenSource cts)
        {
            try
            {
                var token = cts.Token;
                foreach (int number in input.GetConsumingEnumerable())
                {
                    if (token.IsCancellationRequested)
                        break;
                    Thread.Sleep(SHORTSTAGE);
                    result.Add(number);
                    Console.WriteLine("            Stage 4 processed number {0} - Time: {1}", number, st.ElapsedMilliseconds);
                }
            }
            catch (Exception e)
            {
                cts.Cancel();
                if (!(e is OperationCanceledException))
                {
                    Console.WriteLine("            Unexpected exception occured in stage 4");
                    throw;
                }
                else
                    Console.WriteLine("            Operation cancelled in stage 4");
            }
            finally
            {
                Console.WriteLine("Stage 4 stopped");
                
            }
            }

        private static int[] InitializeData(int size)
        {
            var data = new int[size];
            for (int i = 0; i < size; i++)
            {
                data[i] = i;
            }
            return data;
        }

        private static void PrintResult()
        {
            Console.WriteLine("Result:");
            foreach (var item in result)
            {
                Console.Write("{0} ", item);
            }
            Console.ReadLine();
        }
    }
}
