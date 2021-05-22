using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Diagnostics;
using Microsoft.Spark.Sql;
using System.Linq;

namespace TP_3._Final
{
    class Program
    {

        public static ArrayList[] Lecture()
        {
            ArrayList liste_quizz = new ArrayList();
            ArrayList liste_solution = new ArrayList();

            string nomFich = "C:\\Users\\Romuald\\Desktop\\COURS\\Cloud computing\\1_million_sudoku.csv";
            StreamReader fichLect = new StreamReader(nomFich);
            string ligne = "";
            while (fichLect.Peek() > 0)
            {
                ligne = fichLect.ReadLine();
                string[] ligne_lue = ligne.Split(",");
                liste_quizz.Add(ligne_lue[0]);
                liste_solution.Add(ligne_lue[1]);
            }
            fichLect.Close();
            ArrayList[] liste_d_array = new ArrayList[2];
            liste_d_array[0] = liste_quizz;
            liste_d_array[1] = liste_solution;

            return liste_d_array;
        }

        static void To_String(string chaine)
        {
            int i = 0;
            for (int l = 0; l < 9; l++)
            {
                if (l % 3 == 0) Console.WriteLine("--------------------");
                for (int c = 0; c < 9; c++)
                {
                    if (c % 3 == 0) Console.Write("|");
                    Console.Write(chaine[i] + " "); i++;
                }
                Console.Write("\n");
            }
        }

        static List<List<int>> Stocker_val_possible(List<Cellule> list_cell)
        {
            List<List<int>> Liste_sauv_val_possible = new List<List<int>>();
            foreach (Cellule cell in list_cell)
                Liste_sauv_val_possible.Add(cell.valeurs_possibles);
            return Liste_sauv_val_possible;
        }
        
        //Prends en entrée un sudoku non résolu (format string) et renvoie le sudoku résolu sous
        //(format string), et affiche le sudoku résolu, ainsi que le temps  de résolution.
        static string Résolution_Sudoku(string chaine)
        {
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            List<Cellule> Liste0_Cell = new List<Cellule>();
            List<Cellule> Liste_Historique_Cell = new List<Cellule>();
            List<List<int>> Liste0_Int = new List<List<int>>();
            List<List<int>> Liste_Historique_Int = new List<List<int>>();
            int nb_blocage = 0; bool blocage = false;
            Sudoku s1 = new Sudoku();

            while (blocage == false)
            {
                for (int L = 0; L < 9; L++)
                {
                    for (int C = 0; C < 9; C++)
                    {
                        for (int f = 0; f < 2; f++)
                        {
                            s1.Set_matrice(chaine);
                            s1.Reset();
                            s1.Mise_à_jour(Liste_Historique_Cell, Liste_Historique_Int);
                            s1.Ajouter_val_possible();
                            Cellule cell_cible = s1.Cell(L, C);

                            if (cell_cible.valeur_initiale == 0 && cell_cible.valeurs_possibles.Count == 2)
                            {
                                nb_blocage = 0;
                                if (f == 1) cell_cible.valeurs_possibles.Reverse();
                                Liste0_Cell = s1.Retourne_Liste0_Cell(cell_cible);

                                //Profondeur 10 arbitraire choisie
                                s1.Simulation(100000, cell_cible);

                                Liste0_Int = Stocker_val_possible(Liste0_Cell);
                                Liste_Historique_Cell.AddRange(Liste0_Cell);
                                Liste_Historique_Int.AddRange(Liste0_Int);
                            }
                            else { nb_blocage += 1; }
                            if (nb_blocage > 81) blocage = true;
                        }
                    }
                    s1.Tests_triviaux();
                }
            }
            s1.Mise_à_jour2();
            s1.Remplissage_par_test_aléatoire();
            s1.Affiche_Sudoku();

            stopWatch.Stop();

            TimeSpan ts = stopWatch.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
            ts.Hours, ts.Minutes, ts.Seconds,
            ts.Milliseconds / 10);
            Console.WriteLine("Durée exécution code résolution " + elapsedTime);

            return s1.Retourne_string();

        }

        //Chemin du fichier avec 1 000 000 de sudokus
        static readonly string _filePath = System.IO.Path.Combine(@"C:\Users\Romuald\MySparkApp2\sudoku.csv", "sudoku.csv");

        //------------------------------------------------------------------------------------------------------------------------------------


        //Méthode utilisée pour lancer une session spark avec différents nombres
        //de noyaux,d'instances et lancer la résolution du sudoku grace à la méthode Résolution_Sudoku().
        private static void Sudokures(string cores, string nodes, int nrows)
        {
            // Initialisation de la session Spark
            SparkSession spark = SparkSession
                .Builder()
                //.AppName("Resolution of " + nrows + " sudokus using  with " + cores + " cores and " + nodes + " instances")
                .Config("spark.executor.cores", cores)
                .Config("spark.executor.instances", nodes)
                .GetOrCreate();

            // Intégration du csv dans un dataframe
            DataFrame df = spark
                .Read()
                .Option("header", true)
                .Option("inferSchema", true)
                .Csv(_filePath);

            //limit du dataframe avec un nombre de ligne prédéfini lors de l'appel de la fonction
            DataFrame df2 = df.Limit(nrows);

            //Watch seulement pour la résolution des sudokus
            var watch2 = new System.Diagnostics.Stopwatch();
            watch2.Start();

            // Création de la spark User Defined Function
            spark.Udf().Register<string, string>(
                "SukoduUDF",
                (sudoku) => Résolution_Sudoku(sudoku));

            // Appel de l'UDF dans un nouveau dataframe spark qui contiendra les résultats aussi
            df2.CreateOrReplaceTempView("Resolved");
            DataFrame sqlDf = spark.Sql("SELECT Sudokus, SukoduUDF(Sudokus) as Resolution from Resolved");
            sqlDf.Show();

            watch2.Stop();

            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine($"Execution Time for " + nrows + " sudoku resolution with " + cores + " core and " + nodes + " instance: " + watch2.ElapsedMilliseconds + " ms");
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine();

            spark.Stop();

        }


        //-----------------------------------------------------------------------------------------------------------------------------------


        static void Main(string[] args)
        {

            //Temps d'exécution général (chargement du CSV, Création DF, Sparksession)
            var watch = new System.Diagnostics.Stopwatch();
            var watch1 = new System.Diagnostics.Stopwatch();

            watch.Start();

            //Appel de la méthode, spark session avec 1 noyau et 1 instance, 500 sudokus à résoudre
            Sudokures("1", "1", 500);

            watch.Stop();
            watch1.Start();

            //Appel de la méthode, spark session avec 1 noyau et 4 instances, 500 sudokus à résoudre
            Sudokures("1", "4", 500);

            watch1.Stop();

            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine($"Global Execution (CSV + DF + SparkSession) Time with 1 core and 1 instance: {watch.ElapsedMilliseconds} ms");
            Console.WriteLine($"Global Execution (CSV + DF + SparkSession) Time with 1 core and 4 instances: {watch1.ElapsedMilliseconds} ms");
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine();
        }


        //------------------------------------------------------------------------------------------------------------------------------------

    }
}
