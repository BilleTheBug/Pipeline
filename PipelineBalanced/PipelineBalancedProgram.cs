using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace PipelineBalanced
{
    class PipelineBalancedProgram
    {
        private static readonly int SHORTSTAGE = 100;
        private static readonly int LONGSTAGE = SHORTSTAGE*2;
        private static readonly int ARRAYSIZE = 100;
        private static readonly int BUFFERSIZE = 15;

        private static int stage2QueueMax;
        private static int multi1QueueMax;
        private static int multi2QueueMax;
        private static int stage3QueueMax;
        private static int stage4QueueMax;
        private static int lookAheadMax;

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
                var data = InitializeData(ARRAYSIZE);

                var stage1To2Buffer = new BlockingCollection<int>(BUFFERSIZE);
                var stage2ToMultiplexerBuffer1 = new BlockingCollection<int>(BUFFERSIZE);
                var stage2ToMultiplexerBuffer2 = new BlockingCollection<int>(BUFFERSIZE);
                var multiplexerToStage3Buffer = new BlockingCollection<int>(BUFFERSIZE);
                var stage3To4Buffer = new BlockingCollection<int>(BUFFERSIZE);

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
                    if (stage2QueueMax < input.Count)
                    stage2QueueMax = input.Count;
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
                bool roundRobin = true;
                var nextExpected = 1;
                int number = -1;
                var lookAhead = new HashSet<int>();
                while (!(input1.IsCompleted && input2.IsCompleted))
                {
                    if (token.IsCancellationRequested)
                        break;
                    if (multi1QueueMax < input1.Count)
                        multi1QueueMax = input1.Count;
                    if (multi2QueueMax < input2.Count)
                        multi2QueueMax = input2.Count;
                    if (roundRobin)
                    {
                        input2.TryTake(out number);
                    }
                    else
                        input1.TryTake(out number);
                    roundRobin = !roundRobin;
                    if (number == nextExpected)
                    {
                        output.Add(number);
                        nextExpected++;
                        Console.WriteLine("Multiplexer added number {0}", number);
                    }
                    else if (lookAhead.Contains(nextExpected))
                    {
                        lookAhead.Remove(nextExpected);
                        output.Add(nextExpected, token);
                        nextExpected++;
                        lookAhead.Add(number);
                    }
                    else
                        lookAhead.Add(number);
                    if (lookAheadMax < lookAhead.Count)
                        lookAheadMax = lookAhead.Count;
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
                    if (stage3QueueMax < input.Count)
                        stage3QueueMax = input.Count;
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
                    if (stage4QueueMax < input.Count)
                        stage4QueueMax = input.Count;
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
                st.Stop();
            }
            }

        private static int[] InitializeData(int size)
        {
            var data = new int[size];
            for (int i = 1; i < size + 1; i++)
            {
                data[i - 1] = i;
            }
            return data;
        }

        private static void PrintResult()
        {
            Console.WriteLine("\n\n\nResult:");
            int line = 0;
            foreach (var item in result)
            {
                Console.Write("{0} ", item);
                line++;
                    if(line == 10)
                {
                    Console.Write("\n");
                    line = 0;
                }
            }
            Console.WriteLine("\n\nTotal time taken: {0}ms", st.ElapsedMilliseconds);
            Console.WriteLine("\nMax number of elements in buffers:\nBuffer between stage 1 and 2: {0}\nMultiplexer buffer 1: {1}\nMultiplexer buffer 2: {2}\n" +
                "Buffer between multiplexer and stage 3: {3}\nBuffer between stage 3 and 4  3: {4}\nLookAhead buffer: {5}",
                stage2QueueMax, multi1QueueMax, multi2QueueMax, stage3QueueMax, stage4QueueMax, lookAheadMax);
            Console.ReadLine();
        }
    }
}
