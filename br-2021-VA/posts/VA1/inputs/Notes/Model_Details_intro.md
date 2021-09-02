### Modeling State Legislative Elections
For each of the 435 *congressional* districts (CDs) in the U.S., we have:

- The Census Bureau’s Current Population Survey Voter Registration Supplement (CPSVRS)
which contains self-reported voter turnout for about 1% of eligible voters in each district,
broken down by sex (female or male), education (non-college-grad or college-grad)
and race (Black, Latinx, Asian, white-non-Latinx, other).

- The Cooperative Election Study, containing validated turnout and voter preference–specifically
the political party of the voters choice for congressional representative, broken down by the same
demographic categories.

- Population density, computed by aggregating from the Census Bureau’s American Community Survey
(ACS) at the Public-Use-Microdata-Area level.

For each *State Legislative District* (SLD) we have data from the ACS, which we aggregate from
the block-group level using
[areal interpolation](https://medium.com/spatial-data-science/spatial-interpolation-with-python-a60b52f16cbb).
Our shapefiles for the SLDs come from the Census Bureau.  The result of this aggregation is a breakdown
of the citizens in each district by the same demographic variables as the CPSVRS and CES data, as well as
an estimate of population density in the SLD.

Modeling proceeds as follows:

- We combine the CPSVRS, CES, and CD-level population density into one data-set, with rows for each combination
of demographic variables within each CD.  That’s 2 x 2 x 5 x 435 = 8700 rows, each with turnout data from both
CPSVRS and CES, party preference from the CES and population density from the ACS.
