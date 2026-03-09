
# Biological Interaction Graph Specification

Build a graph representing relationships between:

proteins
ligands
genes
pathways
complexes

Graph nodes:

Protein
Ligand
Gene
Pathway
ProteinComplex

Graph edges:

ProteinProteinInteraction
ProteinLigandInteraction
GeneProtein
ProteinPathway
LigandSimilarity

Interaction databases (≈35):

STRING
BioGRID
IntAct
DIP
MINT
Reactome
PathwayCommons
KEGG
SIGNOR
OmniPath
IID
HPRD
HIPPIE
MatrixDB
CORUM
ComplexPortal
PINA
iRefIndex
APID
InnateDB
TRRUST
ChEA
PhosphoSitePlus
DrugBank
BindingDB
ChEMBL
STITCH
GuideToPharmacology
TCRD
OpenTargets
HumanNet
BioPlex
HuRI
PrePPI
PROPER

Graph builder must map identifiers:

UniProt
Entrez
Ensembl

