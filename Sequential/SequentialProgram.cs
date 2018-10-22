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
        private static readonly int LONGSTAGE = 200;
        private static readonly int ARRAYSIZE = 1000;

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
            Thread.Sleep(LONGSTAGE);
            Console.WriteLine("            Stage 4 processed number {0} - time: {1}", number, st.ElapsedMilliseconds);
            result.Add(number);
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
