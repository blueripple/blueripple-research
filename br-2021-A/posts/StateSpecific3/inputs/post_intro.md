# Turnout Gaps and Congressional Districts
In our [last research post](LINK) we used [CPS survey data](LINK) to model differences in turnout between
voters-of-color (VOC) and white-non-Hispanic voters (WHNV).  We looked at each state in
the 2020 general election
and tried to isolate the part of the turnout gap attributable to the state itself rather than
the demographics of that state. Though the data in each demographic group and state is
relatively small, using [MRP](LINK) we’re able to make reasonable inferences by “borrowing
strength”, using things shared among voters in various groups to help improve the precision
of the estimates[^LastPost].

[^LastPost]: Please see [this post](LINK) for more details about the data and model used
here.

In this post we extend that analysis to congressional districts within a few key states.
These estimates are less precise: few people are surveyed within each demographic group and
each district.  But we are fairly confident that the model tells us something about
the *range* of the gaps among the districts in a state and which districts have larger/smaller
gaps than others.

For example, below we chart our model of the district-specific VOC/WHNV turnout gap
in Georgia’s 14 congressional districts. The difference in turnout between
VOC and WHNV was 6 pts higher in GA
than expected based on national turnout and the demographics of GA.  We can see that
roughly in the picture below as the average of the 14 district-specific gaps.
The gaps have a 21 point range, from +16 in GA-3 to -5 in GA-9. While our model
of each gap is uncertain, we are fairly confident that the range of gaps in GA is
roughly 20 points and that GA-3 and GA-11 have better relative turnout of VOC vs.
WHNV than GA-9 and probably GA-5.
