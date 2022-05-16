## 2. Districts worthy of Dem donor support
Based on the results above, we think there are five good options
for Dem donors in MI: MI-3, MI-4, MI-7, MI-8, and MI-10.
We think MI-6 is safe, though we’re going to keep an eye on any polling there.

## 3.	Coming next from Blue Ripple

Here’s where we’re planning to take these analyses over the next few months:

- We’re going to do the same type analysis in many (all?) of the states,
in order to identify the best options for Dem donors in 2022 on both
offense and defense nationwide. Here are our takes on
[Arizona][AZPost],
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

[AZPost]: https://blueripple.github.io/research/NewMaps/AZ_Congressional/post.html
[MIPost]: https://blueripple.github.io/research/NewMaps/MI_Congressional/post.html
[NCPost]: https://blueripple.github.io/research/NewMaps/NC_Congressional/post.html
[PAPost]: https://blueripple.github.io/research/NewMaps/PA_Congressional/post.html
[TXPost]: https://blueripple.github.io/research/NewMaps/TX_Congressional/post.html

[Twitter]: https://twitter.com/BlueRipplePol
[Facebook]: https://www.facebook.com/blueripplepolitics
[Github]: https://github.com/blueripple
[Email]: http://eepurl.com/gzmeQ5

## 4. Coda #1: Demographics of new vs. old MI districts
One thing we haven’t seen discussed very much is how redistricting in MI
has changed the demographics in each district. As a way of putting the
demographic model results in context, let’s look at the underlying
population two different ways:

- The first chart below shows each of MI’s proposed 2022 districts,
with the population broken down by race/ethnicity (Black, Hispanic, Asian,
White-non-Hispanic and other) and education (college graduate and
non-college graduate).
Each bar also has a dot representing the (logarithmic) population density[^popDens]
of the district.
The scale for that dot is on the right-side axis of the chart.
For reference, a log density of 5 represents about 150 people per square mile and a
log density of 8 represents about 3000 people per square mile.
We’ve ordered the districts by D-share based on our demographic model,
which is helpful for understanding how the model responds to demographics and density.

- In the second chart, we look at these demographics a different way,
placing each MI district according to its proportion of college graduates
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
