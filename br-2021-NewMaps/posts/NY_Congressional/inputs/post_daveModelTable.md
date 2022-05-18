## 2. Why Are These Models So Different?
Our findings in NY-11, a swingy district which includes Staten Island,
provide a good opportunity to discuss why BR’s demographic
model and the historical model may differ. In the old map, this
district included Staten Island and some of Sunset Park in Brooklyn.
The struck-down Dem map added very D-leaning Park Slope into
the district, making it a safe-D seat.  The new map removes Park Slope
and slightly changes the boundaries in southwestern Brooklyn. Under the old
map, Democrat Max Rose won this seat in the D-wave of 2018, and promptly lost
it to Republican Nicole Malliotakis in 2020.

NY-11 looks like a R-leaning tossup
given the voting patterns in the precincts within it,
but our demographic model suggests it’s D+17!
What might this mean? One way to answer this question is to consider the difference between the two models.
Our demographic model asks: if the voting-age citizens of this district turned out and voted like similar people
in other parts of the country,  what would we expect the outcome of this election to be?
Whereas the historical model asks how we'd expect the election to turn out if the voters in this district turn out and
vote as they have in previous elections. This points to a few possible reasons why a historically
tossup district like NY-11 might look so strongly D in our model–including, but not limited to, the following:

- Our model may be wrong about how we define "similar" voters. We've incorporated factors like education and race,
  but maybe we've missed key things that make voters in NY-11
  different from superficially "similar" voters in other districts nationwide.

- Location-specific factors may suppress Dem voting. E.g., perhaps the Democratic party or local
  organizations are particularly poorly-organized in NY-11, or voter suppression plays a large role
  and that is reflected in the historical model using those voters. Location specific history
  also matters: Staten Island
  is, historically, a very R leaning part of New York City and our model does not account for that.

- Democrats in NY-11 may, in fact, have underperformed relative to their potential. Or there may have
  been demographic shifts in the district since the last election which favor Democrats. NY-11’s
  Citizen-Voting-Age-Population (CVAP) is about 45% non-White
  (about an even mix of Hispanic and Asian CVAP, as well as a smaller but significant Black CVAP) and
  such districts are usually good for Dem candidates.

We don't know which (if any) of these explanations is correct. But our model suggests that NY-11
might be an easier pickup for Dems than the history indicates.

## 3. BlueRipple on Other States

- Here are our takes on the new maps in
[Arizona][AZPost],
[Michigan][MIPost],
[North Carolina][NCPost],
[Pennsylvania][PAPost],
and [Texas][TXPost].

- We’re going to continue to refine and improve our demographic model–we’ll
update this post and others as we do so. Feel free to contact us if you want
more details on the mechanics, or if you’d like to propose changes or improvements.
- As maps get solidified, we’ll set up ActBlue donation links for candidates
(after the primaries) to make it easy for you to donate.

If you want to stay up-to-date, please [sign up][email] for our email updates!
We’re also on [Twitter][Twitter], [Facebook][Facebook],
and [Github][Github].

[Twitter]: https://twitter.com/BlueRipplePol
[Facebook]: https://www.facebook.com/blueripplepolitics
[Github]: https://github.com/blueripple
[Email]: http://eepurl.com/gzmeQ5

[AZPost]: https://blueripple.github.io/research/NewMaps/AZ_Congressional/post.html
[MIPost]: https://blueripple.github.io/research/NewMaps/MI_Congressional/post.html
[NCPost]: https://blueripple.github.io/research/NewMaps/NC_Congressional/post.html
[PAPost]: https://blueripple.github.io/research/NewMaps/PA_Congressional/post.html
[TXPost]: https://blueripple.github.io/research/NewMaps/TX_Congressional/post.html

## 4. Coda #1: Demographics of new vs. old NY districts
One thing we haven’t seen discussed very much is how redistricting in NY
has changed the demographics in each district. As a way of putting the
demographic model results in context, let’s look at the underlying
population two different ways:

- The first chart below shows each of NY’s proposed 2022 districts,
with the population broken down by race/ethnicity (Black, Hispanic, Asian,
White-non-Hispanic and other) and education (college graduate and
non-college graduate).
Each bar also has a dot representing the (logarithmic) population density[^popDens]
of the district.
The scale for that dot is on the right-side axis of the chart.
For reference, a log density of 5 (NY-21) represents about 150 people per square mile and a
log density of 11.5 (NY-12 and NY-13) represents about 100,000 people per square mile.
We’ve ordered the districts by D-share based on our demographic model,
which is helpful for understanding how the model responds to demographics and density.

- In the second chart, we look at these demographics a different way,
placing each NY district according to its proportion of college graduates
and non-white citizens of voting age. We also indicate (logarithmic)
population density via the size of the circle and modeled D-edge (D-share minus 50%)
via color. This makes it easier to see that the model predicts larger D vote-share
as the district becomes more educated, more non-white and more dense.

[^popDens]: We use logarithms here because
density varies tremendously over districts, from tens to hundreds of thousands of people per square mile.
We use population-weighting because the resulting average more closely expresses
the density of where people actually live.  For example, consider a district made up of a high-density
city where 90% of the population live and then large but low-density exurbs where the other 10% live.
Most people in that district live at high density and we want our density to reflect that even though
the unweighted average density (people/district size) might be smaller.
