using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Sequential
{
    class SequentialProgram
    {
        private static readonly int SHORTSTAGE = 100;
        private static readonly int LONGSTAGE = SHORTSTAGE*2;
        private static readonly int ARRAYSIZE = 100;

        private static Stopwatch st = new Stopwatch();
        private static List<int> result = new List<int>();
        static void Main(string[] args)
        {
            Sequential();
            PrintResult();
        }

        private static void Sequential()
        {
            int[] data = InitializeData(ARRAYSIZE);
            st.Restart();
            foreach (var number in data)
            {
                Stage1(number);
                Stage2(number);
                Stage3(number);
                Stage4(number);
            }
            st.Stop();
        }

        private static void Stage1(int number)
        {
            Thread.Sleep(SHORTSTAGE);
            Console.WriteLine("Stage 1 processed number {0}", number);
        }

        private static void Stage2(int number)
        {
            Thread.Sleep(LONGSTAGE);
            Console.WriteLine("    Stage 2 processed number {0}", number);
        }

        private static void Stage3(int number)
        {
            Thread.Sleep(SHORTSTAGE);
            Console.WriteLine("        Stage 3 processed number {0}", number);
        }

        private static void Stage4(int number)
        {
            Thread.Sleep(SHORTSTAGE);
            Console.WriteLine("            Stage 4 processed number {0} - time: {1}", 
                number, st.ElapsedMilliseconds);
            result.Add(number);
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
            Console.WriteLine("Total time: {0}ms", st.ElapsedMilliseconds);
            Console.WriteLine("Result:");
            int line = 0;
            foreach (var item in result)
            {
                Console.Write("{0} ", item);
                line++;
                if (line == 10)
                {
                    Console.Write("\n");
                    line = 0;
                }
            }
            Console.ReadLine();
        }
    }
}
