---
title: 'fars_cleaner: A Python package for downloading and processing vehicle fatality data in the US'
tags:
  - Python
  - FARS
  - NHTSA
  - vehicle crash analysis
  - crashes
  - road safety
authors:
  - name: Mitchell Z. Abrams
    orcid: 0000-0001-6818-5214
    affiliation: 1
  - name: Cameron R. Bass
    affiliation: 1
affiliations:
 - name: Duke University
   index: 1
date: 25 June 2020
bibliography: paper.bib
---

# Summary

The Fatality Analysis Reporting System (FARS) is a database documenting all vehicle
fatalities in the United States since 1975. The FARS dataset is used toinform safety 
decisions at the local, state and national levels, and provides key insights into the
efficacy of changing vehicle and trafficway safety standards @RN256. However, the 
coding scheme used for many fields has changed through the years, leading to difficulty
in comparing data from year to year. Currently, researchers interested in exploring
the data must manually download .zip files for each year of interest from the 
National Highway Transportation Safety Administration's website, and reference the 
annually-issued Analytical User's Manual to decode the downloaded files.  

`fars_cleaner` is a Python package which aims to solve some of these issues. This
package provides a simple API for downloading and pre-processing the FARS dataset in 
such a way that simplifies comparisons across time. Users can download the FARS data,
and 
`fars_cleaner` delivers data to the user as Pandas DataFrames, 

Additionally, `fars_cleaner` provides an interface to analyze the dataset using the 
Double Pair comparison method, optionally enabling the user to implement a modified 
version with improved variance calculations. 

`fars_cleaner` has been used  @ircobi2020

# Citations

Citations to entries in paper.bib should be in
[rMarkdown](http://rmarkdown.rstudio.com/authoring_bibliographies_and_citations.html)
format.

If you want to cite a software repository URL (e.g. something on GitHub without a preferred
citation) then you can do it with the example BibTeX entry below for @fidgit.

For a quick reference, the following citation commands can be used:
- `@author:2001`  ->  "Author et al. (2001)"
- `[@author:2001]` -> "(Author et al., 2001)"
- `[@author1:2001; @author2:2001]` -> "(Author1 et al., 2001; Author2 et al., 2002)"

# Figures

Figures can be included like this:
![Caption for example figure.\label{fig:example}](figure.png)
and referenced from text using \autoref{fig:example}.

Fenced code blocks are rendered with syntax highlighting:
```python
for n in range(10):
    yield f(n)
```	

# Acknowledgements

We acknowledge contributions from Brigitta Sipocz, Syrtis Major, and Semyeong
Oh, and support from Kathryn Johnston during the genesis of this project.

# References