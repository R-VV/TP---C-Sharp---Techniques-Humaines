using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TP_3._Final
{
    class Sudoku
    {

        //ATTRIBUT 

        private Cellule[,] matrice = new Cellule[9, 9];


        /* CONSTRUCTEUR */

        public Sudoku() { }

        public Sudoku(Cellule[,] Matrice)
        {
            this.matrice = Matrice;
        }

        public Cellule[,] Get_matrice()
        {
            return this.matrice;
        }



        /* METHODES */

        public void Set_matrice(string liste_val_initiales)
        {
            int incrémenteur = 0;
            for (int l = 0; l < 9; l++)
            {
                for (int c = 0; c < 9; c++)
                {
                    matrice[l, c] = new Cellule(l, c, (int)Char.GetNumericValue((char)liste_val_initiales[incrémenteur]), new List<int>());
                    incrémenteur++;
                }
            }
        }

        public void Affiche_Sudoku()
        {
            for (int l = 0; l < 9; l++)
            {
                if (l % 3 == 0) Console.WriteLine("--------------------");
                for (int c = 0; c < 9; c++)
                {
                    if (c % 3 == 0) Console.Write("|");
                    Console.Write(matrice[l, c].valeur_initiale + " ");
                }
                Console.Write("\n");
            }
        }
        public List<int> AntiDoublon(List<int> liste1, List<int> liste2)
        {
            List<int> sortie = new List<int>();
            for (int i = 0; i < liste1.Count; i++)
            {
                bool test = false;
                for (int j = 0; j < liste2.Count; j++)
                {
                    if ((int)liste1[i] == (int)liste2[j])
                    {
                        test = true;
                        break;
                    }
                }
                if (test == false) sortie.Add(liste1[i]);
            }
            return sortie;
        }
        public List<int> AntiContradiction(List<int> sortie, List<int> liste1, List<int> liste2, List<int> liste3)
        {
            List<int> liste_sortie = new List<int>();
            foreach (int entier in sortie)
                if (est_présent(entier, liste1) == false && est_présent(entier, liste2) == false && est_présent(entier, liste3) == false) liste_sortie.Add(entier);
            return liste_sortie;
        }

        //Mets à jour les attributs "valeurs possibles" des objets cellules de la matrice, en fonction de leur nouvel environnement
        public void Ajouter_val_possible()
        {
            ArrayList instructions_colonne = new ArrayList { 0, 0, 1, 1, 1, 0, -1, -1, -1 };
            ArrayList instructions_ligne = new ArrayList { 0, -1, -1, 0, 1, 1, 1, 0, -1 };

            for (int ligne = 1; ligne < 9; ligne += 3)
            {
                for (int colonne = 1; colonne < 9; colonne += 3)
                {
                    List<int> liste_val_carré = new List<int>();
                    for (int i = 0; i < 9; i++)
                    {
                        int y = ligne + (int)instructions_ligne[i];
                        int x = colonne + (int)instructions_colonne[i];
                        liste_val_carré.Add(matrice[y, x].valeur_initiale);
                    }
                    for (int i = 0; i < 9; i++)
                    {
                        int y = ligne + (int)instructions_ligne[i];
                        int x = colonne + (int)instructions_colonne[i];

                        List<int> liste_val_colonne = new List<int>();
                        for (int index1 = 0; index1 < 9; index1++)
                        {
                            liste_val_colonne.Add(matrice[index1, x].valeur_initiale);
                        }
                        List<int> liste_val_ligne = new List<int>();
                        for (int index2 = 0; index2 < 9; index2++)
                        {
                            liste_val_ligne.Add(matrice[y, index2].valeur_initiale);
                        }
                        List<int> val_générales = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
                        List<int> sortie = new List<int>();
                        List<int> sortie_nulle = new List<int> { 0 };

                        sortie = AntiDoublon(val_générales, liste_val_ligne);
                        sortie = AntiDoublon(sortie, liste_val_colonne);
                        sortie = AntiDoublon(sortie, liste_val_carré);
                        sortie = AntiContradiction(sortie, liste_val_ligne, liste_val_colonne, liste_val_carré);
                        if (matrice[y, x].valeur_initiale == 0) matrice[y, x].valeurs_possibles = sortie;
                        else
                        {
                            matrice[y, x].valeurs_possibles = sortie_nulle;
                        }
                    }
                }
            }
        }

        public Cellule Cell(int Ligne, int Colonne)
        {
            return matrice[Ligne, Colonne];
        }

        //Renvoie le booléen Vrai si la valeur val est présente dans la liste
        public bool est_présent(int val, List<int> liste)
        {
            bool test = false;
            for (int i = 0; i < liste.Count; i++)
            {
                if ((int)liste[i] == val) test = true;
            }
            return test;
        }

        public List<Cellule> AntiDoublon2(List<Cellule> liste)
        {
            List<Cellule> sortie = new List<Cellule>();
            if (liste.Count > 1)
            {
                sortie.Add(liste[0]);
                for (int j = 1; j < liste.Count; j++)
                {
                    bool test = false;
                    for (int i = 0; i < sortie.Count; i++)
                    {
                        if ((Cellule)liste[j] == (Cellule)sortie[i]) test = true;
                    }
                    if (test == false) sortie.Add(liste[j]);
                }
            }
            if (liste.Count == 1)
            {
                sortie.Add(liste[0]);
            }
            return sortie;
        }

        //Retourne sous forme d'une liste de Cellules, les cellules étant dans la même ligne,colonne,carré que la cellule choisie
        public List<Cellule> Zone_influence(Cellule cellule, bool critère = false)
        {
            int val1 = 0;

            val1 = (int)cellule.valeurs_possibles[0];
            List<Cellule> Liste = new List<Cellule>();

            for (int i = 0; i < 9; i++)
            {
                //Zone d'influence colonne
                if (est_présent(val1, matrice[i, cellule.colonne].valeurs_possibles) == true)
                {
                    Liste.Add(matrice[i, (int)cellule.colonne]);
                }
                //Zone d'influence ligne
                if (est_présent(val1, matrice[cellule.ligne, i].valeurs_possibles) == true)
                {
                    Liste.Add(matrice[cellule.ligne, i]);
                }
            }
            //Zone d'influence carré
            int ligne_carré = cellule.ligne - cellule.ligne % 3;
            int colonne_carré = cellule.colonne - cellule.colonne % 3;
            for (int a = 0; a < 3; a++)
            {
                for (int b = 0; b < 3; b++)
                {
                    if (est_présent(val1, matrice[a + ligne_carré, b + colonne_carré].valeurs_possibles) == true)
                    {
                        Liste.Add(matrice[a + ligne_carré, b + colonne_carré]);
                    }
                }
            }
            // Sélection des cellules ayant 2 valeurs possibles uniquement
            if (critère == true)
            {
                List<Cellule> Liste2 = new List<Cellule>();
                foreach (Cellule element in Liste)
                {
                    if (element.valeurs_possibles.Count == 2) Liste2.Add(element);
                }
                Liste.Clear();
                Liste = Liste2;
            }
            Liste = AntiDoublon2(Liste);
            if (Liste.IndexOf(cellule) != -1) Liste.Remove(cellule);
            return Liste;
        }


        public List<Cellule> Retourne_Liste0_Cell(Cellule cell_cible)
        {
            return Zone_influence(matrice[cell_cible.ligne, cell_cible.colonne]);
        }

        public List<List<int>> Retourne_Liste0_Int(List<Cellule> cell_liste)
        {
            List<List<int>> Liste0_Int = new List<List<int>>();
            foreach (Cellule cell in cell_liste)
                Liste0_Int.Add(cell.valeurs_possibles);
            return Liste0_Int;
        }

        public bool Test_zone_influence(Cellule cell1, Cellule cell2)
        {
            bool test = false;
            if (cell1.ligne == cell2.ligne) test = true;
            if (cell1.colonne == cell2.colonne) test = true;
            int ligne_carré_1 = cell1.ligne - cell1.ligne % 3;
            int ligne_carré_2 = cell2.ligne - cell2.ligne % 3;
            int colonne_carré_1 = cell1.colonne - cell1.colonne % 3;
            int colonne_carré_2 = cell2.colonne - cell2.colonne % 3;
            if (ligne_carré_1 == ligne_carré_2 && colonne_carré_1 == colonne_carré_2) test = true;
            return test;
        }

        public List<Cellule> Rafraîchir(List<Cellule> Liste)
        {
            foreach (Cellule element in Liste)
                element.valeurs_possibles = matrice[element.ligne, element.colonne].valeurs_possibles;

            return Liste;
        }

        public bool est_présent_cell(Cellule cell, List<Cellule> Liste)
        {
            bool test = false;
            foreach (Cellule element in Liste)
                if (cell == element) test = true;

            return test;
        }

        public List<Cellule> Cloner_List(List<Cellule> Liste)
        {
            List<Cellule> Liste2 = new List<Cellule>();
            foreach (Cellule element in Liste)
                Liste2.Add(element);

            return Liste2;
        }

        //Vérifie si la cellule de L1 présentée à équivaut la valeur test commune à tout L0
        public bool est_modifiable(Cellule cell_L1, int val_test)
        {
            bool test = false;
            if (val_test == (int)cell_L1.valeurs_possibles[0]) test = true;
            return test;
        }

        public List<Cellule> Suppression_déduction2(List<Cellule> Liste1, List<Cellule> Liste0, int val_test)
        {
            List<Cellule> Liste2 = new List<Cellule>();
            List<Cellule> Liste1_influence = new List<Cellule>();
            int total = Liste1.Count;

            foreach (Cellule cell_liste1 in Liste1)
            {
                // 1ère étape : vérification avec L0 pour voir si interaction possible
                foreach (Cellule cell_liste0 in Liste0)
                {
                    if (Test_zone_influence(cell_liste0, cell_liste1) == true && est_modifiable(cell_liste1, val_test) == true)
                    {
                        matrice[cell_liste0.ligne, cell_liste0.colonne].valeurs_possibles.Remove(cell_liste1.valeurs_possibles[0]);
                        Liste0 = Rafraîchir(Liste0);
                    }
                }
                // 2e étape : déploiement
                Liste1_influence = Zone_influence(cell_liste1, true); // Cellule ayant obligatoirement 2 valeurs possibles uniquement (dont 1 commune)

                foreach (Cellule cell_influencée_liste1 in Liste1_influence)
                {
                    if (est_présent_cell(cell_influencée_liste1, Liste0) == false)
                    {
                        matrice[cell_influencée_liste1.ligne, cell_influencée_liste1.colonne].valeurs_possibles.Remove(cell_liste1.valeurs_possibles[0]);
                        Liste2.Add(matrice[cell_influencée_liste1.ligne, cell_influencée_liste1.colonne]);
                    }
                }
            }
            return Liste2;
        }
        public void Simulation(int lg_chaine, Cellule cell)
        {
            List<Cellule> Liste0 = new List<Cellule>();
            List<Cellule> Liste1 = new List<Cellule>();
            List<Cellule> Liste2 = new List<Cellule>();

            //Valeur testée pour cette case du sodoku
            int val_test = (int)matrice[cell.ligne, cell.colonne].valeurs_possibles[0];

            //Zone influence 1er element de la cellule g
            Liste0 = Zone_influence(matrice[cell.ligne, cell.colonne]);

            //Pour une prise en condidération de la 2e valeur, suppression de la première
            matrice[cell.ligne, cell.colonne].valeurs_possibles.RemoveAt(0); //Pour que l'opération suivante considère la 2e valeur

            //Ajout de la première cellule g à la Liste1
            Liste1.Add(matrice[cell.ligne, cell.colonne]);

            for (int i = 0; i < lg_chaine; i++)
            {
                Liste2 = Suppression_déduction2(Liste1, Liste0, val_test);
                Liste1.Clear();
                Liste1 = Cloner_List(Liste2);
                Liste2.Clear();
            }
        }

        public bool Mise_à_jour(List<Cellule> Liste0_Cell, List<List<int>> Liste0_Int)
        {
            bool test = false;
            for (int i = 0; i < Liste0_Cell.Count; i++)
            {
                if (Liste0_Int[i].Count == 1)
                {
                    matrice[Liste0_Cell[i].ligne, Liste0_Cell[i].colonne].valeurs_possibles.Clear();
                    matrice[Liste0_Cell[i].ligne, Liste0_Cell[i].colonne].valeur_initiale = (int)Liste0_Int[i][0];
                    test = true;
                }
                else
                {
                    matrice[Liste0_Cell[i].ligne, Liste0_Cell[i].colonne].valeurs_possibles = Liste0_Int[i];
                }
            }
            return test;
        }
        public void Reset()
        {
            for (int l = 0; l < 9; l++)
            {
                for (int c = 0; c < 9; c++)
                {
                    matrice[l, c].valeurs_possibles.Clear();
                }
            }
        }

        static void Affiche_Sudoku(Cellule[,] matrice)
        {
            for (int l = 0; l < 9; l++)
            {
                if (l % 3 == 0) Console.WriteLine("--------------------");
                for (int c = 0; c < 9; c++)
                {
                    if (c % 3 == 0) Console.Write("|");
                    Console.Write(matrice[l, c].valeur_initiale + " ");
                }
                Console.Write("\n");
            }
        }

        //Remplir les lignes, colonnes, carrés ayant 1 unique valeur manquante, par l'unique valeur logique possible restante
        public void Tests_triviaux()
        {
            bool blocage = false;
            List<int> liste_nb = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8, 9 }; int nb_blocage = 0; int nb_blocage2 = 0;
            while (blocage == false)
            {
                for (int l = 0; l < 9; l++)
                {
                    int nb_zero_L = 0; int nb_zero_C = 0;
                    List<int> liste_ligne = new List<int>(); List<int> liste_colonne = new List<int>();
                    for (int c = 0; c < 9; c++)
                    {
                        liste_ligne.Add(matrice[l, c].valeur_initiale);
                        liste_colonne.Add(matrice[c, l].valeur_initiale);
                        if (matrice[l, c].valeur_initiale == 0) nb_zero_L += 1;
                        if (matrice[c, l].valeur_initiale == 0) nb_zero_C += 1;
                    }
                    if (nb_zero_L == 1)
                    { matrice[l, liste_ligne.IndexOf(0)].valeur_initiale = AntiDoublon(liste_nb, liste_ligne)[0]; nb_blocage = 0; }
                    if (nb_zero_C == 1)
                    { matrice[liste_colonne.IndexOf(0), l].valeur_initiale = AntiDoublon(liste_nb, liste_colonne)[0]; nb_blocage = 0; }
                    nb_blocage += 1;
                }
                if (nb_blocage > 10) blocage = true;
            }

            for (int L = 0; L < 7; L += 3)
                for (int C = 0; C < 7; C += 3)
                {
                    int nb_zero_Car = 0; List<int> liste_carré = new List<int>();
                    for (int li = 0; li < 3; li++)
                    {
                        for (int col = 0; col < 3; col++)
                        {
                            liste_carré.Add(matrice[L + li, C + col].valeur_initiale);
                            if (matrice[L + li, C + col].valeur_initiale == 0) nb_zero_Car += 1;
                        }
                    }
                    int position_ligne = L + (int)Math.Floor((decimal)(int)liste_carré.IndexOf(0) * 1 / 3);
                    int position_colonne = C + (int)(liste_carré.IndexOf(0) % 3);
                    if (nb_zero_Car == 1)
                    { matrice[position_ligne, position_colonne].valeur_initiale = AntiDoublon(liste_nb, liste_carré)[0]; nb_blocage2 = 0; }
                    else { nb_blocage2 += 1; }
                }
        }

        public void Mise_à_jour2()
        {
            for (int l = 0; l < 9; l++)
            {
                for (int c = 0; c < 9; c++)
                {
                    if (matrice[l, c].valeurs_possibles.Count == 1 && matrice[l, c].valeur_initiale == 0)
                    {
                        matrice[l, c].valeur_initiale = matrice[l, c].valeurs_possibles[0];
                        matrice[l, c].valeurs_possibles.Clear();
                    }
                }
            }
        }

        //Vérifie s'il n'y pas plusieurs valeurs identiques pour chaque ligne, colonne, carré
        public bool Verif_matrice_correct()
        {
            List<int> liste_ligne = new List<int>(); List<int> liste_colonne = new List<int>(); bool propre = true;
            for (int l = 0; l < 9; l++)
            {
                liste_ligne.Clear(); liste_colonne.Clear();
                liste_ligne.Add(matrice[l, 0].valeur_initiale);
                liste_colonne.Add(matrice[0, l].valeur_initiale);
                for (int c = 1; c < 9; c++)
                {
                    if (matrice[l, c].valeur_initiale != 0)
                    {
                        if (est_présent((int)matrice[l, c].valeur_initiale, (List<int>)liste_ligne) == true) propre = false;
                        liste_ligne.Add(matrice[l, c].valeur_initiale);
                    }
                    if (matrice[c, l].valeur_initiale != 0)
                    {
                        if (est_présent((int)matrice[c, l].valeur_initiale, liste_colonne) == true) propre = false;
                        liste_colonne.Add(matrice[c, l].valeur_initiale);
                    }
                }
            }
            ArrayList instructions_colonne = new ArrayList { 0, 1, 1, 1, 0, -1, -1, -1 };
            ArrayList instructions_ligne = new ArrayList { -1, -1, 0, 1, 1, 1, 0, -1 };
            List<int> liste_carré = new List<int>(); int l_pos = 0; int c_pos = 0;

            for (int li = 1; li < 9; li += 3)
            {
                for (int ci = 1; ci < 9; ci += 3)
                {
                    liste_carré.Clear();
                    liste_carré.Add(matrice[li, ci].valeur_initiale);
                    for (int i = 0; i < 8; i++)
                    {
                        l_pos = li + (int)instructions_ligne[i];
                        c_pos = ci + (int)instructions_colonne[i];
                        if (matrice[l_pos, c_pos].valeur_initiale != 0 && est_présent(matrice[l_pos, c_pos].valeur_initiale, liste_carré) == true) propre = false;
                        if (matrice[l_pos, c_pos].valeur_initiale != 0) liste_carré.Add(matrice[l_pos, c_pos].valeur_initiale);
                    }
                }
            }
            return propre;
        }

        //Retourne le sudoku au format string à partir du format sudoku objet
        public string Retourne_string()
        {
            string chaine = "";
            for (int ligne = 0; ligne < 9; ligne++)
            {
                for (int colonne = 0; colonne < 9; colonne++)
                {
                    chaine += (matrice[ligne, colonne].valeur_initiale).ToString() ;
                }
            }        
            return chaine;
        }


        //Sert à remplir les toutes dernières valeurs restantes du Sudoku (au nombre de 4 ou en général),
        //non résolues par la méthode XY Chain, en essayant aléatoirement les valeurs
        //données dans l'attribut "valeur possible" des cellules restantes, jusqu'à ce que le Sudoku ne présente plus d'erreurs et soit résolu.
        public void Remplissage_par_test_aléatoire()
        {
            bool condition = false;

            List<Cellule> liste_cellule = new List<Cellule>();
            for (int l = 0; l < 9; l++)
            {
                for (int c = 0; c < 9; c++)
                {
                    if (matrice[l, c].valeur_initiale == 0) liste_cellule.Add(matrice[l, c]);
                }
            }
            while (condition == false)
            {
                Random alea = new Random();
                foreach (Cellule cell in liste_cellule)
                {
                    int pos_alea = alea.Next(0, cell.valeurs_possibles.Count);
                    cell.valeur_initiale = cell.valeurs_possibles[pos_alea];
                }
                if (Verif_matrice_correct() == true) condition = true;
            }
        }
    }
}

