As a way of understanding the model results, let’s look at the demographics two different ways.
The chart below shows each of NC’s proposed 2022 districts, with the population broken down by race/ethnicity
(Black, Hispanic, Asian, white-non-Hispanic and other) and education (College Grad and non-College Grad).
Each bar also has a dot representing the (logarithmic) population density[^popDens] of the district.
The scale for that dot is on the right-side axis of the chart.  For reference, a log density of 5 represents
about 150 people per square mile and a log density of 8 represents about 3000 people per square mile.

We’ve ordered the districts by the modeled D-share which is helpful for understanding how the model responds
to demographics and density.

[^popDens]: We use logarithms here because
density varies tremendously over districts, from tens to hundreds of thousands of people per square mile.
We use population-weighting because the resulting average more closely expresses
the density of where people actually live.  For example, consider a district made up of a high-density
city where 90% of the population live and then large but low-density exurbs where the other 10% live.
Most people in that district live at high density and we want our density to reflect that even though
the unweighted average density (people/district size) might be smaller.
