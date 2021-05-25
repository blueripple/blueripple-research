# State-Level Voter Turnout And Race

### Key Points In This Post

- Voters-of-color (VOC) often vote at lower rates than White-non-Hispanic voters (WNHV).
This "turnout gap" varies strongly between states.
Different VOC (younger or older, male or female, level of education and race/ethnicity)
vote at different rates.  Is this the source of the state-to-state variation?
- We model 2020 general election turnout, concluding that state-to-state variations in the
VOC turnout are *unlikely* to be explained by demographics alone.
- State-specific VOC turnout was high in CO, AL, PA, GA and MI and low in
WA, IL, MA, NH and FL.

## Focusing on State-Specific Turnout Effects

As we've written about [before](https://blueripple.github.io/research/mrp-model/p3/main.html),
voter turnout varies widely among demographic groups.
Younger voters and voters of color
tend to vote for Democrats, but smaller fractions of those voters cast a vote in most elections.
Similarly, White voters without a college degree, who
are more likely to vote for Republicans, are less likely to vote than White voters
with a college degree.

Unsurprisingly, group-level turnout varies by state:
each state has a different mix of eligible voters, with different
distributions of age and education, etc.  We are interested in the
impact of state-level policies on turnout of VOC vs WNHV, so
we look at the turnout gap, the difference between VOC turnout and
WNHV turnout–positive “gaps” indicate higher turnout among VOC, negative
indicate higher turnout among WHNV[^1].

[^1]: We could look instead at the state-specific
    effect on VOC turnout alone, but variations among states in *overall* turnout
    makes this difficult to interpret.

Below we chart the 2020 turnout gap in each state
along with the average gap
(about -9%, marked by a vertical orange line)
to illustrate the range seen among the states (and DC).  Since these
turnout estimates are produced from a model[^2] we also
include 90% confidence intervals for our estimates.

[^2]: As we’ll discuss below, the data on the demographic makeup of voters is sparse.
    So we don’t *know* the demographics of all voters. We use an MRP model of that sparse
    data to infer the demographics of all the voters, and these inferences have uncertaintly,
    reflected in the confidence intervals.
