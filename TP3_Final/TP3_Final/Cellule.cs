using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace TP_3._Final
{
    class Cellule
    {
        /* ATTRIBUT */

        public int ligne { get; set; }
        public int colonne { get; set; }
        public int valeur_initiale { get; set; }
        public List<int> valeurs_possibles { get; set; }

        /*CONSTRUCTEUR*/

        public Cellule() { }

        public Cellule(int valeur_initiale, List<int> valeurs_possibles)
        {
            this.valeur_initiale = valeur_initiale;
            this.valeurs_possibles = valeurs_possibles;
        }

        public Cellule(int Ligne, int Colonne, int valeur_initiale)
        {
            this.ligne = Ligne;
            this.colonne = Colonne;
            this.valeur_initiale = valeur_initiale;
        }

        public Cellule(int Ligne, int Colonne, int valeur_initiale, List<int> valeurs_possibles)
        {
            this.ligne = Ligne;
            this.colonne = Colonne;
            this.valeur_initiale = valeur_initiale;
            this.valeurs_possibles = valeurs_possibles;
        }


        // METHODES

        public Cellule Clone()
        {
            Cellule c1 = new Cellule();
            c1.ligne = this.ligne;
            c1.colonne = this.colonne;
            c1.valeurs_possibles = this.valeurs_possibles;
            c1.valeur_initiale = this.valeur_initiale;
            return c1;
        }

        public void affiche_propriétés()
        {
            Console.WriteLine("ligne " + ligne + " ; colonne " + colonne + " ; valeur initiale " + valeur_initiale);
            Console.WriteLine("valeurs possibles");
            print_array(valeurs_possibles);
        }

        public void print_array(List<int> array)
        {
            foreach (int element in array)
            {
                Console.Write(element + " ");
            }
            Console.WriteLine("");
        }
    }
}
