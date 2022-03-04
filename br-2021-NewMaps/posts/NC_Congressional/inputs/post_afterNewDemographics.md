It’s hard to see anything specific from these charts, though we are
continuing to examine them as we try to understand what might be happening
in each specific district. Overall, our impression is that this new map in NC
has made the safe D districts safer by adding voters-of-color
(mostly Black voters but also some Hispanic and Asian voters) and thus
made the rest of the districts easier for Republicans to win by removing those
same voters from places where they might have made districts competitive.

## 5. Coda #2: Brief intro to our methods (for non-experts)
This part of the post contains a general summary of the math behind what we’re
doing here intended for non-experts. If you want even more technical details,
check out the links at the end of this section,
visit our Github page, or contact us directly.

As we’ve discussed before, we refer to our model as a demographic model.
We use turnout and voting data (from the [CES][CES] survey) based on education,
sex, race, population density and state to estimate expected turnout and voter
preference for various types of people in each state.
Then we look at the demographics of a particular district
(using tract-level census data from the [ACS][ACS]), breaking it down into
the same categories and then apply our model of turnout and voter preference
to estimate the 2-party vote share we expect for a Democratic candidate.[^old]

[^old]: As a point of comparison we also run the model and apply it to the 2020
map, this time comparing to the election results.  For those results, and some
demographic details of the old districts, click [here][oldDistricts].

This is in contrast to what we call the historical model: a
standard way to predict “partisan lean” for any district,
old or new: break it into precincts with known voting history
(usually a combination of recent presidential, senate and governors races)
and then aggregate those results to estimate expected results in the district.

The historical model is likely to be a pretty accurate “predictor” if you think
the same people will vote the same way in subsequent elections,
regardless of where the district lines lie. So why did we build a
demographic model? Three reasons:

1.	We’re interested in places where the history may be misleading, either because of the specific story
in a district or because changing politics or demographics may have altered the balance of likely voters
but one or both parties are not seeing the threat or new opportunity.[^empowerment]

2.	Our demographic analysis is potentially more useful when the districts are new,
since voting history may be less “sticky” there. For example, if I’m a Dem-leaning voter
in a strong-D district, I might not have bothered voting much in the past because I
figured my vote didn’t matter. But if I now live in a district that’s more competitive
in the new map, I might be much more likely to turn out.

3.	We’re fundamentally interested not so much in predicting what will happen in each district,
but what plausibly could happen in each district if Dems applied resources in the right way.
The historical model is backward-looking, whereas our demographic model is forward-looking
and we think it’s a better guide to inform strategy.

Two final points. First, when it comes to potential Dem share in each district,
we’re continuing to improve and refine our demographic model. The Blue Ripple
web-site contains more details on [how it works][methods] and
some [prior results][VASLModel] of applying a similar model to state legislative
districts, something we will also do more of in the near future.
Second, for the historical model comparator, we use data from the excellent
[“Dave’s Redistricting”][DavesR],
which is also the source of our maps for the new and old districts.

[^empowerment]:	We’re also interested in voter empowerment strategies. In particular,
questions about where and among whom, extra turnout might make a difference.
The historical model is no help here since it does not attempt to figure out who is voting
or who they are voting for in a demographically specific way.

[DavesR]: https://davesredistricting.org/maps#aboutus
[methods]: https://blueripple.github.io/explainer/model/ElectionModel/post.html
[VASLModel]: https://blueripple.github.io/research/StateLeg/VA1/post.html
[CES]: https://cces.gov.harvard.edu
[ACS]: https://www.census.gov/programs-surveys/acs/
