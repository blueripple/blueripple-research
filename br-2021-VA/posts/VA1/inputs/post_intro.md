# Eyes on the State Houses: VA edition

It’s abundantly clear that Dems and progressives have suffered major setbacks when Republicans control
statehouses. On the voting rights front, it’s opened the door to partisan and race-based gerrymandering[REF]
and voter suppression[REF].  On hundreds of substantive issues, including abortion access[REF],
Covid-safety[REF], medicaid expansion[REF], and education policy[REF],
Republican control is an ongoing disaster for Democratic policy positions and, more
importantly, for the people of these states. We’ve written before about the importance of state-legislative
elections [REF] and, if anything, it has only become more urgent since.

VA, which holds elections for their lower house this November, is the rare good-news story in this regard:
the lower house went blue in 2019, after 20 years of Republican control,
and the state senate followed in 2020.

We’d like to keep it that way. State-legislative elections are challenging from a data
perspective: polling is almost non-existent
and demographic information gets more difficult to find for smaller regions, making any kind of
forecasting difficult. From our perspective, this makes it hard for us to filter
the huge number of races by our number one criterion: winnability.

But, because we think this is so important, we decided to dive in anyway.
Below we’ll look at our
first attempt to model expected election results in the VA lower house.
In this post we’ll model what we would have forecast in 2019
and compare it to the outcome.
To be very clear: we’re not interested in predicting outcomes. We are interested in
which races might be winnable, or which seats more in need of defending and we
imagine those are the same as seats which look close when predicted.

### Key Points In This Post

- Using voter turnout data and validated voter preference we can model expected
turnout and preference for a specific region if we have enough demographic information.
- Demographic information for state-legislative districts (SLDs) is available from the
ACS (American Community Survey) and the decennial census.
- Combining this information allows us to estimate the outcome of an election in
a SLD.
- A quick attempt to to do this using 2018 data and compare to 2019 results is encouraging.

## Modeling Election Results from Demographic Information
Election models typically use demographic *and* additional
information about past elections, economic growth, etc.
But past election data can be very heavily
influenced by candidate quality (or someone running unopposed), a real issue
in state-legislative elections. It’s also not clear how much right-way/wrong-way
feelings about the economy can explain local election results. For those reasons,
and to keep things simple, we’re going to stick to demographic varriables. The model
in this post uses population density, Sex (restricted to binary Male or Female because that’s what
is in the data we have), education level (non-college-grad or college-grad)
and race (Black, Latinx, Asian, white-non-Latinx, other). We also include a
factor for the state. We would very much like to have an age factor as well but the tables
made available by the census at the SLD level preclude this[^whyNoAge].

[^whyNoAge]: For larger geographic areas, it’s possible to get ACS “micro-data,”
which allows combined information for many factors.  But once we get down to the
block-group level, we can only get the data as tabulated by the census and they
do not provide 4-factor tables. So we had to limit ourselves to sex, education and
race. Once more decennial data from 2020 is available,
we may be able to improve on this.

We assemble SLD-level demographic information using census provided
shapefiles for each
district. The shapefile is used to find all the block-groups inside the
district and those are aggregated[^demographicCode].

[^demographicCode]: We built a small python script to automate most
of this process. We download shapefiles and block-group-data for the
state and the script merges those into SLD-level demogrpahics.  The
code is available on our github site[LINK].

Further complicating things, our best source for turnout data, the census
bureau’s Current Population Survey Voting and Registration Supplement (CPSVRS),
has no data about who voters chose in the election, just whether or not they
voted.  Our chosen source for preference information,
the Cooperative Election Survey (CES) also has turnout information but it’s
not considered as reliable as the CPSVRS.

Our model combines those data sets at the congressional district level,
jointly estimates, via multi-level regression[REF],
turnout probabilities using counts from the CPSVRS and
D preference from the CES. We then post-stratify[REF] the estimates across
the demographic data we built for each VA lower house district. The result is
a prediction for the expected D vote share in each district.

Cutting to the chase: in the chart below we plot the model estimate
(using 2018 data) vs. the results of the 2019 election. In blue,
we also plot the model=result line,
where every dot would fall if the model were perfect, and a regression
line in red (contested races only; $R^2 = 0.75$)
to show how much explanatory power the model has.
The model is far from perfect but nonetheless
extremely informative. The uncontested races fall on the sides of the chart
and we can see that these are predictably one-sided in the model,
with the exception of district
78 (a swingy district that was uncontested by the Democrats).
