In each of these districts, our model and the historical average differ by about 20 points,
with the exception of CA-2 where we underestimate the D share by nearly 30 points! What’s going on?

These misses fall into a few categories:

- CA-2 (Under by 29), CA-12 (under by 19), CA-11 (under by 18) are all majority White districts with
very high educational attainment. CA-11 and CA-12 are also relatively high density, over 10,000 ppl
per sq mile. Though the model has state-level coefficients, it still doesn’t capture how D-leaning
most White people in CA are. This has some overlap with our next category.

- WI-2 (under by 20) and CO-2 (under by 17)–and to some extent CA-2–are “college-towns,”
places where a good fraction of the
voting age population are college students. We’re not sure how to account for this but these
districts do appear to lean more D than the model estimates from demographics alone.

- TX-37 (under by 22), TX-35 (under by 18) and AZ-7 (under by 17) are all districts with large numbers
of people identifying as ethnically Hispanic. There are real and large
uncertainties about Hispanic voters, compounded by lumping them all together. For example,
voters who identify as Cuban and Venezualan vote differently from voters who identify as
Mexican or Puerto Rican, and these differences vary by state.

- NY-11 (over by 20) and CO-5 (over by 17) are majority White places but not enough to explain the
voting history. One possible explanation for CO-5 is religion, as it’s home to a large evangelical
population. It also has a large military population and a number of defense contractors, all populations
that skew more Republican than our demographics capture.
NY-11 is harder to explain. It contains Staten Island, where White voters
are substantially more conservative than most of the rest of the city.

We have a few planned improvements that may help.
We hope to add age into the model. The ACS provides data in such a way that for each census
tract you cannot count educational attainment, age, race and sex simultaneously. But there are methods
to *infer* the age information by looking at counts of age, sex, and race and counts of education,
sex, and race together. We plan to use those techniques to bring some age information to bear. We
may also attempt to refine our picture of Hispanic voters, either including country of origin, racial
identification–some Hispanic voters identify as White and some do not and that may be predictive.

Our most ambitious upgrade would involve using congressional
districts rather than states as our samllest geographical when estimating the model,
capturing more local effects. This would not *explain* why Staten Island differs from the rest of NYC,
but it would capture that difference within the model, allowing us to see its demmographic components
more clearly. The best way to do this would be to have election results and demographics for
every voting precinct. That data is not readily
available, though we think [OpenPrecincts][openPrecints] is close.
Converting that data–maps with embedded
election results–to the form we need is time consuming. On top of that, modeling at
that geographic scale is more computationally intensive, complicating the process required to find an
appropriate model. And this would complicate using the model
for state-leg districts since they are not neccessarily nested in congressional districts.
Still, we plan to try this approach, including precinct data where available.

[openPrecincts]: https://openprecincts.org
