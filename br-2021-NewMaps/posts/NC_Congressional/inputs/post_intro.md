# Blue Ripple Model and NC’s new Congressional Map

About a month ago, Frank and I spoke to Dale about, among other things, looking at new
congressional maps as they come out in case our demographic models are at all
useful for your candidate selection work.

We’ve spent time on this since then, figuring out where to
find new maps, determining the best sources for good voting-history-based
analysis of new districts
and updating each of our data sources where
applicable. Each election cycle we update our model and workflow. This year is our first with
newly drawn maps. We can no longer use the congressional district coding
already present in our turnout and voting data. Instead we turn to shapefiles for the
new districts and project existing census data onto those regions. Some of those pieces
of workflow were new to us.

Our model is demographic and geographic. We use turnout and voting data
(from the [CCES survey][CCES]) based on education, sex, race, population density and state
to estimate expected turnout and voter preference for various types of people in
each state.  Then we look at the demographics of a particular district (using tract-level census
data from the [ACS][ACS]), breaking it down
into the same categories and use our model to estimate the 2-party vote share we
expect for a democratic candidate. We’ll call this the
“demographic model”.

[CCES]: https://cces.gov.harvard.edu
[ACS]: https://www.census.gov/programs-surveys/acs/

The standard way to predict “partisan lean” for any district, old or new:
break it into precincts with known voting history (usually a combination of
recent presidential, house and governor races) and then aggregate those results to
estimate expected results in the district.
We’ll call this the “historical model.”

The historical model is likely to be more accurate as a predictive tool since
the same people often vote the same way in subsequent elections.
So why build a demographic model? We’re interested in
places where the history may be misleading, either because of the specific story
in a district or because changing politics or demographics may have altered
the balance of likely voters but one or both parties are not seeing the threat or new
opportunity.[^empowerment]

[^empowerment]: We’re also interested in voter empowerment strategies.
In particular, questions about where and among whom, extra turnout might make a difference.
The historical model is no help here since it does not attempt to figure out who is voting
or who they are voting for in a demographically specific way.

This sort of analysis is potentially more useful when the districts are new,
since voting history may be less “sticky” there.

We decided to take your interest in NC as a good excuse to walk carefully through the model
as applied to a new map, using NC as a test-case.

To start, we first look to the old districts. This serves to orient us and ensure the model makes
some sense.  Before we apply the model to these districts, let’s look at the demographics.  The chart
below shows each of NC’s 2020 districts, with the population broken down by race/ethnicity
(Black, Latinx, Asian, white-non-Latinx and other) and education (College Grad and non-College Grad).
Each bar also has a dot representing the (logarithmic) population density[^popDens] of the district.
The scale for that dot is on the right-side axis of the chart.  For reference, a log density of 5 represents
about 150 people per square mile and a log density of 8 represents about 3000 people per square mile.

[^popDens]: We use logarithms here because
density varies tremendously over districts, from tens to hundreds of thousands of people per square mile.
We use population-weighting because the resulting average more closely expresses
the density of where people actually live.  For example, consider a district made up of a high-density
city where 90% of the population live and then large but low-density exurbs where the other 10% live.
Most people in that district live at high density and we want our density to reflect that even though
the unweighted average density (people/district size) might be smaller.
