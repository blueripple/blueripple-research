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
recent presidential, house and governors races) and then aggregate those results to
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

We started by looking at the old districts. This serves to orient us and ensure the model makes
some sense. If you’re curious, that analysis is [here][old districts].  The upshot is that the
model holds up fairly well when compared to the 2020 election results, though it is notably
more optimistic about D share in very Republican districts than it should be and more
pessimistic about D-share in very Democratic districts.  We suspect this is an issue about
how we handle population density and we will update this note if we make any improvements.

For brevity’s sake, let’s cut to the chase! Below we chart the model as applied to the *new*
districts. We compare this to a traditional historical analysis from the
excellent [“Dave’s Redistricting”][Daves], also the source of our map for the new districts.

[Daves]: https://davesredistricting.org/maps#home

The Dave’s Redistricting “partisan lean” (converted to 2-party D vote share) is on the x-axis
and our model estimate on the y-axis,
as well as a line representing where districts would fall on this scatter-plot if the model
and election result agreed precisely.  We’ve also included confidence intervals from the model.
In districts left of the line, the model overrestimated the D vote share and
underestimated it in districts to the right.

NB: For this and all scatter charts to follow, you
can pan & zoom by dragging with the mouse or moving the scroll wheel.  To reset the chart,
hold shift and click with the mouse.
