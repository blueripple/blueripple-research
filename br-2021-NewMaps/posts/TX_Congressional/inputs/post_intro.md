Blue Ripple Model and TX new Congressional Map

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

[^popDens]: We use logarithms here because
density varies tremendously over districts, from tens to hundreds of thousands of people per square mile.
We use population-weighting because the resulting average more closely expresses
the density of where people actually live.  For example, consider a district made up of a high-density
city where 90% of the population live and then large but low-density exurbs where the other 10% live.
Most people in that district live at high density and we want our density to reflect that even though
the unweighted average density (people/district size) might be smaller.
