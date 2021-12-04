# Blue Ripple Model and NC’s new Congressional Map

About a month ago, Frank and I spoke to Dale about, among other things, looking at new
congressional maps as they come out and seeing if our view is at all
useful for your candidate selection work.

We’ve spent some time on this since then, mostly doing the preliminary work of figuring out where to
find new maps, finding sources for good conventional analysis and updating our data sources where
applicable.  Each election cycle we update our model and workflow, and this year is our first with
new maps. That’s a small data challenge since we can no longer use the congressional district coding
already present in our turnout and voting data. Instead we need to get shapefiles for the
new districts and project existing census data onto those regions.

Our model is entirely demographic and geographic. We use turnout and voting data
(from the CCES survey) based on education, sex, race, population density and state
to estimate  expected turnout and voter preference for various types people in
each state.  Then we look at the demographics of a particular district, breaking it down
into the same categories and then using our model to estimate the 2-party vote share we
expect for a democratic candidate. We’ll call this the
“demographic model”.

The standard way to predict “partisan lean” for any district, old or new:
break it into precincts with known voting history and then aggregate those.
We’ll call this the “historical model.”
We think the historical model is likely to be more accurate as a predictive tool.
So why build a demographic model? We’re interested in places where the demographic model and
the historical one disagree. We think that is one way to spot places which may not
have been competitive but should be or are becoming competitive, leading to better
allocation of resources to candidates at the *edges* of the list of competitive districts.
In other words, we hope our work can help add some candidates in districts that look
historically uncompetitive but demographically competitive or drop some in districts which
look historically competitive but demographically safe or out of reach.

This sort of analysis is potentially more useful when the districts are new,
since voting history may be less “sticky” in new districts.

We decided to take your interest in NC as a good excuse to walk carefully through the model and
we’re going to take you along!

To start, we look first to the old districts. This serves to orient us and make sure the model makes
some sense.  But before we even model these districts, let’s look at the demographics.  The chart
below shows each of NC’s 13 2020 districts, with the population broken down by race/ethinicity
(Black, Latinx, Asian, white-non-Latinx and other) and education (College Grad and non-College Grad).
Each bar also has a dot representing the *relative* population density of the district. We’ll talk more about
population density below but it turns out to be tricky to use in a model but also essential to
understanding voting behavior.
-
