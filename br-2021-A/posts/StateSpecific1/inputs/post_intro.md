# State-Level Voter Turnout And Race

### Key Points In This Post

- Voters-of-color (VOC) often vote at lower rates than white-non-Hispanic voters (WNHV).
  This “turnout gap” varies strongly among states.
- One reason states’ turnout gaps may differ is demographic: different subgroups of VOC/WNHV
  (differing, for ecample, in age, sex, level of education and/or race/ethnicity)
  vote at different rates, so the precise demographic makeup in a particular state matters.
  But other factors,
  like state-specific voting policies and strength of get-out-the-vote organizing, could also
  play a role.
- Our model of 2020 general election turnout—based on turnout data from the census—
  suggests that non-demographic factors
  are a large component of the state-to-state variability in the gap between VOC
  and WNHV turnout.
- These non-demographic, state-specific factors shrunk or reversed turnout gaps
  in CO, AL, PA, GA and MI and widened them in WA, IL, MA, NH and FL.

## Focusing on State-Specific Turnout Effects

As we've written about [before](https://blueripple.github.io/research/mrp-model/p3/main.html),
voter turnout varies widely among demographic groups.
Younger voters and voters of color
tend to vote for Democrats, but smaller fractions of those voters cast a vote in most elections.
Similarly, white voters without a college degree, who
are more likely to vote for Republicans, are less likely to vote than white voters
with a college degree.

Unsurprisingly, group-level turnout varies by state:
each state has a different mix of eligible voters, with different
distributions of age and education, etc.  We are interested in the
impact of *state-level* differences (policy, organizing, local candidates or ballot initiatives)
on turnout of VOC and WNHV, so
we look at the turnout “gap”, the difference between VOC turnout and
WNHV turnout—positive gaps indicate higher turnout among VOC, negative
indicate higher turnout among WHNV[^whyGaps].

[^whyGaps]: We could look instead at the state-specific
    effect on VOC turnout alone, but variations among states in *overall* turnout
    makes this difficult to interpret.

Below we chart the 2020 turnout gap in each state
along with the average gap
(about -9%, marked by a vertical orange line)
to illustrate the range seen among the states (and DC).  Since these
turnout estimates are produced from a model[^whyModel] we also
include 90% confidence intervals for our estimates.

[^whyModel]: As we’ll discuss below, the CPS data on the demographic makeup of voters is sparse.
    So we don’t *know* the demographics of all voters. We use an MRP model of that sparse
    data to infer the demographics of all the voters, and these inferences have uncertainty,
    reflected in the confidence intervals.  There are other sources of uncertainty
    (survey methods, etc.) that we are not quantifying here,
    so these confidence intervals are probably too small.
    [Click here][niNaiveModel_link] for a comparison between our MRP model and a much
    simpler model of the same data.
