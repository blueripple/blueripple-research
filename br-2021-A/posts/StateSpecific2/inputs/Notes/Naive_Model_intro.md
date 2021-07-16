### Does It Need To Be So Complicated?

Do we need an MRP model just to find turnout gaps from a
demographically labeled set of turnout data?  Can’t we just count
WHNV and VOC and compare?  We can, but there are a few issues:

1. People constructing the survey will try to sample a representative
group of people, representative nationwide and representative in each state.
But that’s hard to do!  And since fewer than 1000 people are surveyed in some
states, it’s very unlikely that it will be representative along all the
dimensions we care about (age, sex, race, education).  We can try to
address this shortcoming with post-stratification. However,

2. We used MR to “borrow strength,”
for example, using turnout by age data for the entire country to help estimate
the turnout by age in each state. Otherwise, the number of people available
to estimate leads to very large uncertainties.

Let’s make this clearer by doing a simple estimation of each state turnout
gap and comparing it to our MRP estimates of the same gaps. In each state
we count the eligible VOC and how many cast votes and the same for WHNV.
Then we assume each group is representative in each state and so
we take the ratio of votes to eligible voters as the turnout probability
for each voter (we model voter turnout with the
[Binomial distribution][^Binomial]);
the difference of those probabilities is the turnout gap.  We
make the simplifying assumption that the VOC turnout rate and WHNV
turnout rate are uncorrelated (this is probably not true!). So we
“add”[^quadrature] the Binomial uncertainties to get an uncertainty
for the difference.

[^quadrature]: Actually, we add them in “quadrature”:
the uncertainty of the difference of turnout rates is
the square root of the sum of the squares of the
uncertainties of each turnout rate.

Charted below is this simple model of turnout gaps and uncertainties
along with the same thing from the MRP model.  You’ll notice that
they roughly agree, though not always, and that the uncertainties
in the simple model are much larger.

[^Binomial]: Suppose you do something over and over
with the same probability of success, for example, flipping a
coin and trying to get “heads,” or counting the votes from
voters identically likely to vote.
The [Binomial distribution][Wiki:Binomial] describes the
probability of getting some number of successes in some
larger number of attempts. If we have 30 votes among 40 voters,
our estimate of the probability is 75%.  What the binomial
distribution provides is an estimate of the uncertainty,
which for probability of success p, is $\sqrt{p(1-p)}$

[Wiki:Binomial]: <https://en.wikipedia.org/wiki/Binomial_distribution>
