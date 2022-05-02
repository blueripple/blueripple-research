This part of the post contains a general summary of the math behind what we’re
doing here intended for non-experts. If you want even more technical details,
check out the links at the end of this section,
visit our Github page, or contact us directly.

Our model is demographic.
We use turnout data from the 2020 [CPS][CPS] voter supplement
(a self-reported survey);
voting and turnout data from the 2020 [CES][CES] (a validated survey);
and election result data from the 2020 presidential,
senate and house elections. The survey data from the CPS and CES is
broken down by several demographic categories, including
sex, education and race/ethnicity.

The election results are trickier to use in the model
since we don’t have demographic information paired with
with turnout or vote choice. What we do know is the overall
demographics of the state or house district.
So we use the election-data to assign a likelihood to
the *post-stratification* of our parameters across the demographics
of the relevant region (from the micro-data [ACS][ACS]).

Then we look at the demographics of a particular house or
state-legislative district
(using tract-level census data from the [ACS][ACS]), breaking it down into
the same categories and then apply our model of turnout and voter preference
to estimate the 2-party vote share we expect for a Democratic candidate.

This is in contrast to what we call the historical model: a
standard way to predict “partisan lean” for any district,
old or new: break it into precincts with known voting history
(usually a combination of recent presidential, senate and governors races)
and then aggregate those results to estimate expected results in the district.

The historical model is likely to be a pretty accurate “predictor” if you think
the same people will vote the same way in subsequent elections,
regardless of where the district lines lie. So why did we build a
demographic model? Three reasons:

1.	We’re interested in places where the history may be misleading,
either because of the specific story in a district or because changing
politics or demographics may have altered the balance of likely voters.[^empowerment]

2.	Our demographic analysis is potentially more useful when the districts are new,
since voting history may be less “sticky” there. For example, if I’m a Dem-leaning voter
in a strong-D district, I might not have bothered voting much in the past because I
figured my vote didn’t matter. But if I now live in a district that’s more competitive
in the new map, I might be much more likely to turn out.

3.	We’re not as interested in predicting what will happen in each district,
but what plausibly could happen in each district if Dems applied resources in the
right way, or fail to when the Republicans do.
The historical model is backward-looking, whereas our demographic model is forward-looking
making them complementary when it comes to strategic thinking.

Two final points. First, when it comes to potential Dem share in each district,
we’re continuing to improve and refine our demographic model. The Blue Ripple
web-site contains more details on [how it works][methods] and
some [prior results][VASLModel] of applying a similar model to state legislative
districts, something we will also do more of in the near future.
Second, for the historical model comparator, we use data from the excellent
[“Dave’s Redistricting”][DavesR],
which is also the source of our maps for the new districts.

[^empowerment]:	We’re also interested in voter empowerment strategies. In particular,
questions about where and among whom, extra turnout might make a difference.
The historical model is no help here since it does not attempt to figure out who is voting
or who they are voting for in a demographically specific way.

[^augment]: This matching of election results is actually quite complicated! There are various
ways to do this.  In the literature, this is typically done post-modeling, via an ad-hoc
procedure that tries to make a minimum set of adjustments to the parameters such that the
election results are closely matched.  In our case, we put this into the model itself,
which makes the modeling more time consuming but also provides results which are potentially
more accurate and have more information about uncertainties.

[DavesR]: https://davesredistricting.org/maps#aboutus
[methods]: https://blueripple.github.io/research/StateLeg/VA1/Notes/Model_Details.html
[VASLModel]: https://blueripple.github.io/research/StateLeg/VA1/post.html
[CES]: https://cces.gov.harvard.edu
[CPS]: https://catalog.data.gov/dataset/current-population-survey-voting-and-registration-supplement
[ACS]: https://www.census.gov/programs-surveys/acs/
