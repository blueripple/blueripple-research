{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE DerivingVia               #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE StandaloneDeriving        #-}
{-# LANGUAGE TupleSections             #-}
{-# LANGUAGE UndecidableInstances      #-}
{-# OPTIONS_GHC -O0 -fplugin=Polysemy.Plugin  #-}

module MRP.DistrictClusters where

import qualified Control.Foldl                 as FL
import qualified Control.Foldl.Statistics      as FLS
import qualified Control.Monad.State           as State
import           Data.Discrimination            ( Grouping )
import qualified Data.List as List
import qualified Data.Map as M
import           Data.Maybe (catMaybes, fromMaybe)
import qualified Data.Text                     as T
import qualified Data.Serialize                as Serialize
import qualified Data.Set as Set
import qualified Data.Vector                   as Vec
import qualified Data.Vector.Unboxed           as UVec
import qualified Data.Vector.Storable           as SVec
import qualified Data.Vector.Generic as GVec
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import qualified Data.Massiv.Array as MA

import GHC.Generics (Generic)

import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.CSV as F
import qualified Frames.InCore                 as FI
import qualified Polysemy.Error as P
import qualified Polysemy.RandomFu             as PRF
import qualified Polysemy.ConstraintAbsorber.MonadRandom as PMR

import qualified Control.MapReduce             as MR
import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.SimpleJoins            as FJ
import qualified Frames.Misc                   as FM
import qualified Frames.Serialize              as FS
import qualified Frames.KMeans                 as FK
import qualified Math.KMeans                   as MK
import qualified Math.Rescale                  as MR
import qualified Numeric.LinearAlgebra as LA

import qualified Frames.Visualization.VegaLite.Data
                                               as FV

import qualified Graphics.Vega.VegaLite        as GV
import qualified Knit.Report                   as K

  
import           Data.String.Here               ( i, here )

import qualified Text.Blaze.Html               as BH
import qualified Text.Blaze.Html5.Attributes   as BHA

import           BlueRipple.Configuration 
import           BlueRipple.Utilities.KnitUtils 
import qualified BlueRipple.Utilities.TableUtils as BR

import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.Bootstrap            as GLM
import qualified Numeric.GLM.MixedModel            as GLM

import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Clock               as Time

import qualified Data.Datamining.Clustering.SGM as SGM
import qualified Data.Datamining.Clustering.Classifier as SOM
import qualified Data.Datamining.Clustering.SOM as SOM
import qualified Math.Geometry.Grid as Grid
import qualified Math.Geometry.Grid.Square as Grid
import qualified Math.Geometry.GridMap as GridMap
import qualified Math.Geometry.GridMap.Lazy as GridMap 
import qualified Data.Random.Distribution.Uniform as RandomFu
import qualified Data.Random as RandomFu
import qualified Data.Sparse.SpMatrix as SLA

import qualified Data.Word as Word

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.CPSVoterPUMS as CPS
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET

import qualified BlueRipple.Model.MRP as BR
import qualified BlueRipple.Model.Turnout_MRP as BR

import qualified BlueRipple.Model.TSNE as BR
import qualified Data.Algorithm.TSNE.Utils as TSNE
import qualified Numeric.Clustering.Perplexity as Perplexity

import qualified BlueRipple.Data.UsefulDataJoins as BR
import qualified MRP.CCES_MRP_Analysis as BR
import qualified MRP.CachedModels as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.Keyed         as Keyed
import MRP.Common
import MRP.CCES
import qualified MRP.CCES as CCES


{-
TODO:
1. Color tSNE by meaningful variables.  Starting with all 40 buckets individually.
2. Unweighted NN?
3. Fix "Vote Share" as name
4. Look at logistic regression (MRP fixed-only??) for same buckets.
5. Color tSNE by region?
6. Other variables altogether.  How long has the incumbent been there, etc.
-}

textIntro :: T.Text
textIntro = [here|
Most models of voting behavior rely on demographic variables as predictors. That is, they look at
voters in various demographic buckets (age, race, education, sex, income, etc.), and their votes
as reported to surveys, like the CCES survey, or via voter-files.  From this data, probabilities for
voting for certain candidates based on demographics are inferred and used, along with some weighting
of the electorate---that is, some model of who is likely to vote---to analyze election results or to
predict electoral outcomes.

Underlying this work is the idea that these demographic variables are good predictors of voters behavior.
More sophisticated methods, like MRP, are able to add some geographic variation to these inferences but
the underlying concept is the same.  If you know someone's demographics, you can estimate the probability
that they will vote for a candidate of a given party.

Though models of demographics may be very complex, with hundreds or thousands of possible
combinations of age, sex, etc., these results are often massively simplified when discussed
and presented in the popular press.  So we look at groups like "White Working-Class Men" or
"Black Women" as ways of understanding the dynamics of elections.  For example, Rachel Bitecofer
talks about the importance of "pools of educated voters" in shifting house districts.

We were interested in examining these ideas in a slightly different way.  We start with the
same general idea, bucket people by demographic groups---in our case by age (under or over 45),
sex (male or female), education (non-college-grad or college grad)
and race (Black, Latinx, Asian, White-non-Latinx and other).  We break race down more finely than the
other categories because there is considerable evidence that Black, Latinx, Asian and White-non-Latinx
voters are very distinct in their voting behavior and so merging them into White and non-White
would be insufficient.

Next we tried to understand which house districts were demographically similar to each other.
This is not simple!  Even in our relatively straightforward demographic breakdown, there are 40
numbers describing each district (the percentage of people in the district in each category).  What does
"similar" mean here?

So we tried some things! In each case, we use purely demographic variables to quantify and
visualize some idea of distance between districts and then add 2018 voting results on top
in order to see if demographic similarity leads to electoral-outcome similarity.  As we'll
explain below, we also begin to explore what it means when a district's voting behavior
looks "out-of-place" or "anomalous" when analyzed this way.
|]
  
textPCAPre :: T.Text
textPCAPre = [here|
First, we tried looking at the [principal components][Wikipedia:PCA] of the demographics.
That is, we considered the 438 (districts) row
by 40 (demographic %s) column matrix, $P$, and looked at the eigen-vectors
of the two largest eigen-values of $P^TP$.  These should be the two "directions"
in demographic space which account for the most variation among districts. We
project each district onto these two vectors and plot the result along with the
Dem vote share in the 2018 house race.
This is pictured below.

[Wikipedia:PCA]: <https://en.wikipedia.org/wiki/Principal_component_analysis>
|]


textPCAPost :: T.Text
textPCAPost = [here|
The largest eigenvalue (corresponding to "PC1") is about 20 times larger than the 2nd
(corresponding to "PC2").  The 2nd is about 3 times larger than the 3rd and the spectrum
gets flatter from there.
The actual values of the PCA variables don't mean anything obvious.
With a little examination of the vectors
themselves, it's pretty clear that negative values of "PC1" correspond roughly to
"White Working Class" and positive values of "PC2" correspond roughly to "Black Working Class".

Some examples of how this plays out:  the district in the lower-right corner is HI-1, a district
which is 54% Asian, 19% White and 5% Latinx and 2% Black.  So it's a place with lower
than typical numbers of White and Black working class folks.  By contrast, the lower-leftmost district
(which is hard to see on the chart) is MT-0,
a district that is 91% White, 6% Native American and 2% Latinx. With a median income of about $54,000,
MT-0 is full of White working class people but almost no black people at all. As a last example,
consider GA-5, the district John Lewis's represented until his death
and the district with the largest projection on PC2.
GA-5 is 58% Black, 32% White, 6% Latinx and 5% Asian.
The median income in GA-5 is about $57,000. So GA-5 has many working class Black people and
*comparatively* few white ones.

The most striking thing is how strongly separated the blue (house districts that elected democrats)
are from the red districts (house districts that elected republicans).
Recall that vote-share was not in any way involved in the computation of the principal components.
And yet, the projection of the demographics onto the principal components shows a clear if
hard-to-specify relationship between demographics and election outcome.

Also of note are some districts which seem "out-of-place", for example FL-25, which is
a faint red-dot (PC1 = -52740, PC2=3313) surrounded by blue dots.  FL-25 is 76% Latinx,
which would usually suggest a democratic district, like NJ-8, just below it in the chart.
But over 50% of the Latinx residents of FL-25 are Cuban, and Cubans are much more likely to
vote Republican than other Latinx voters.  Other out-of-place districts may likewise
have district-specific explanations.  But others might be places worth looking for flippable
districts.

Principal components analysis is an example of dimension-reduction and, in the case where
it's used to look at only a small number of components, something we will call an "embedding".
By an "embedding" we mean somehow representing the high-dimensional space in a lower-dimensional
one.  Such a thing has nothing specific to say about how alike or different things are, though
we may try to use the embedded distances that way.  This is different from using "clustering"
to visualize high-dimensional data.  Clustering is entirely about similarity and difference
but often has nothing more specific to say than whether things are in the same cluster.

These methods are often combined; frequently an embedding is then clustered
(using k-means, or something similar) or data is clustered and then embedded, where
the clusters may be more easily viewed.
|]

textSOMPre :: T.Text
textSOMPre = [here|
So let's cluster this data and see what that looks like!  As an example of clustering,
we will use Self-Organizing-Maps (SOMs).  The SOM is an interesting clustering method
because it can preserve a bit more information than just what's in each cluster.  It can
also identify nearby clusters, or more precisely, it may preserve some of the structure
of the high-dimensional data.  To make an SOM, we choose
a few districts at random and "place" them at the points of a 3x3 grid.  Then we use
all the districts to "train" the SOM as follows: one district at a time, we find the
"closest" (here we use the absolute-value of the coordinate differences in
the high-dimensional space) district to it on the grid.  Then we move the district on
the grid closer to the input district.  We do the same, but to a lesser extent with the
nearest neighbors of that point on the grid.  Once the SOM has seen all the districts,
we stop and then use each point of the grid as a cluster by assigning every district to
its closest grid point in the now-rearranged grid.  Since the SOM has some concept
of nearness built in, we make a sort of embedding out of these clusters by plotting them
on the grid.  We assign coordinates by using a weighted average of the inverse distances
to the closest grid district and its nearest neighbors. Like the PCA chart, we assign a
color to each district based on the D vs. R vote-share in the 2018 house elections.
|]

textSOMPost :: T.Text
textSOMPost = [here|
Again we see some very clear separation between blue and red districts and then
some clusters which are mixed.  Even within the mixed clusters, using our ad-hoc
embedding scheme, we see some blue/red separation.  That is, in mixed clusters,
blue districts often "lean" toward a bluer cluster and red ones toward a redder
cluster.  This again, supports our intuition that demographics determines a
great deal of voting behavior.
|]


text_tSNEPre :: T.Text
text_tSNEPre = [here|
One weakness of the PCA embedding was that it made inefficient use of
the 2D space.  It also is prone to showing some things as close that
might actually be fairly far apart, just in directions which are orthogonal
to the PCs we used.  For example, neither of the first 2 PCs had a strong
educational component.

"t-Distributed Stochastic Neighbor Embedding" (tSNE) is a (non-linear) technique for
overcoming some of these issues.  Rather than project the high-dimensional data
directly, tSNE attempts to build a 2D representation which preserves the distribution
of distances to neighbors.  Roughly, tSNE begins by computing, for each district,
a probability distribution over the other districts, where nearness corresponds to
high probability.  Then it builds a
2-dimensional set of coordinates such that these neighbor probabilities are as
close as possible to the original ones.
There's one crucial parameter, called "perplexity" which roughly
stands in for the number of nearest neighbors each district should have.
At low perplexity, only the very nearest neighbors have probabilities much different from 0,
at higher perplexities, more neighbors have non-zero probabilities.  

As with the other charts, on the tSNE chart below,
we add color to each district to show
2-party vote-share.
|]

textAnomaly :: T.Text
textAnomaly = [here|

A detour is in order to talk about where we're trying to go with all this.
Our basic interest is identifying districts which elected a Republican
to the house in the last election
(2018 here) but are demographically more similar to a set of districts which,
on average, elected Democrats.  These districts might be "flippable" in this
election or in the future. We're also interested in the same thing but with
the parties reversed.  Those are districts we might need to work harder to defend.

So far, we have a simple tSNE inspired approach to this.  First we choose
a demographic distance between districts--this could be the original 40-dimensional
distance, which we call "Raw", the distance using the first two
principal components, the distance using the tSNE coordinates,
or the distance on our SOM embedding.  Then, as in the first step of
tSNE, we construct a probability distribution over pairs of districts
such that "closer" districts have higher probability than ones further
apart.  We use those probabilities to construct a distribution
of 2-party vote-shares.  In particular, for the $i$th district,
we compute the mean, $m_i$, and standard
deviation $\sigma_i$ of that distribution.  Then, for the district in question, we
look at the standard-deviation-scaled difference between the 2-party vote
share, $v_i$, in the district and
what is "expected" using the probability weighted neighbors:
$a_i = \frac{v_i - m_i}{\sigma_i}$.

|]
  
textFlip :: T.Text
textFlip = [here|
One issue is immediately apparent.  There are a lot of large dark blue points,
and a few large dark red ones. These are districts where Democrats or Republicans
ran unopposed, thus they are anomalous, but likely not in a very interesting way!
This makes it clear that we really care only about anomalies where $v_i$ and $m_i$ are on
different sides of 50%. Plotting that (which we call the "Flip Index") instead, we get the chart below.
|]

textQuestions :: T.Text = [here|
Some questions:

1. Is this way of looking at "anomalousness" at all useful?  On the one hand, it makes some intuitive sense:
make some sensible definition of "neighborhood" and then use that to consider what's unexpected.  However, tSNE
itself is not a method for detecting outliers since it shifts relative distances.  I don't think that's
what we're doing here, though we are using tSNE inspired probabilities to construct our neighborhood.

2. We're struggling with districts where candidates ran unopposed.  These are outliers but not errors
and they shouldn't be dropped.  Should we transform vote-share somehow?  Or, at the very least, set the vote-share
of the unopposed to the max/min vote-share in a competitive district?  Is there some clever way to use neighborhoods
to set a better edge?

3. Is there a better way than the "Flip Index" idea to focus in on the question of flippability?
|]

textAnomalyVsShare :: T.Text = [here|
Also, we can look directly at anomaly vs 2018 vote-share (and add 2016 vote-share via color):
|]

textEndForNow :: T.Text = [here|
The table below has all this data in tabular form.
|]
  

type ASER5CD as = ('[BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.CongressionalDistrict] V.++ DT.CatColsASER5) V.++ as

{-
addPUMSZerosF :: FL.Fold (F.Record (ASER5CD '[PUMS.Citizens, PUMS.NonCitizens])) (F.FrameRec (ASER5CD '[PUMS.Citizens, PUMS.NonCitizens]))
addPUMSZerosF =
  let zeroPop ::  F.Record '[PUMS.Citizens, PUMS.NonCitizens]
      zeroPop = 0 F.&: 0 F.&: V.RNil
  in FMR.concatFold
     $ FMR.mapReduceFold
     FMR.noUnpack
     (FMR.assignKeysAndData @'[BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.CongressionalDistrict])
     (FMR.makeRecsWithKey id
       $ FMR.ReduceFold
       $ const
       $ Keyed.addDefaultRec @DT.CatColsASER5 zeroPop)
-}                                                                
type PopPct = "Pct Pop" F.:-> Double
type PctWWC = "PctWWC" F.:-> Double
type PctBlack = "PctBlack" F.:-> Double
type PctNonWhite = "PctNonWhite" F.:-> Double
type PctYoung = "PctYoung" F.:-> Double
type Pref2016 = "Pref2016" F.:-> Double
type Cluster k = "Cluster" F.:-> k
type SOM_Cluster = Cluster (Int, Int)
type K_Cluster = Cluster Int
type PC1 = "PC1" F.:-> Double
type PC2 = "PC2" F.:-> Double
type TSNE1 = "tSNE1" F.:-> Double
type TSNE2 = "tSNE2" F.:-> Double
type TSNEIters = "tSNE_iterations" F.:-> Int
type TSNEPerplexity = "tSNE_Perplexity" F.:-> Int
type TSNELearningRate = "tSNE_LearningRate" F.:-> Double

type instance FI.VectorFor (Int, Int) = K.Vector

instance FV.ToVLDataValue (F.ElField SOM_Cluster) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

isWWC r = F.rgetField @DT.Race5C r == DT.R5_WhiteNonLatinx && F.rgetField @DT.CollegeGradC r == DT.Grad
isBlack r =  F.rgetField @DT.Race5C r == DT.R5_Black
isNonWhite r =  F.rgetField @DT.Race5C r /= DT.R5_WhiteNonLatinx
isYoung r = F.rgetField @DT.SimpleAgeC r == DT.Under

type W = "w" F.:-> Double

districtToWWCBlack :: FL.Fold
                        (F.Record (PUMS.CDCounts DT.CatColsASER5))
                        (F.FrameRec '[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, PctWWC, PctBlack, W])
districtToWWCBlack =
  let districtToXYW :: FL.Fold (F.Record (DT.CatColsASER5 V.++ '[PUMS.Citizens])) (F.Record [PctWWC, PctBlack, W])
      districtToXYW =
        let --isWWC r = F.rgetField @DT.Race5C r == DT.R5_WhiteNonLatinx && F.rgetField @DT.CollegeGradC r == DT.Grad
            --isBlack r =  F.rgetField @DT.Race5C r == DT.R5_Black
            citizens = realToFrac . F.rgetField @PUMS.Citizens        
            citizensF = FL.premap citizens FL.sum
            pctWWCF = (/) <$> FL.prefilter isWWC (FL.premap citizens FL.sum) <*> citizensF
            pctBlackF = (/) <$> FL.prefilter isBlack (FL.premap citizens FL.sum) <*> citizensF
        in (\x y w -> x F.&: y F.&: w F.&: V.RNil) <$> pctWWCF <*> pctBlackF <*> citizensF
  in FMR.concatFold
     $ FMR.mapReduceFold
     FMR.noUnpack
     (FMR.assignKeysAndData @'[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict] @(DT.CatColsASER5 V.++ '[PUMS.Citizens]))
     (FMR.foldAndAddKey districtToXYW)

votesToVoteShareF :: FL.Fold (F.Record [ET.Party, ET.Votes]) (F.Record '[ET.PrefType, BR.DemPref])
votesToVoteShareF =
  let
    party = F.rgetField @ET.Party
    votes = F.rgetField @ET.Votes
    demVotesF = FL.prefilter (\r -> party r == ET.Democratic) $ FL.premap votes FL.sum
    demRepVotesF = FL.prefilter (\r -> let p = party r in (p == ET.Democratic || p == ET.Republican)) $ FL.premap votes FL.sum
    demPref d dr = if dr > 0 then realToFrac d/realToFrac dr else 0
    demPrefF = demPref <$> demVotesF <*> demRepVotesF
  in fmap (\x -> FT.recordSingleton ET.VoteShare `V.rappend` FT.recordSingleton @BR.DemPref x) demPrefF


data DistrictP as rs = DistrictP { districtId :: F.Record as, isModel :: Bool, districtPop :: Int, districtVec :: UVec.Vector Double } deriving (Generic)
deriving instance Show (F.Record as) => Show (DistrictP as rs)
deriving instance Eq (F.Record as) => Eq (DistrictP as rs)
--instance S.Serialize (DistrictP as rs)

vecDemoAsRecs :: forall ks d.
                  (Ord (F.Record ks)
                  , Keyed.FiniteSet (F.Record ks)
                  , V.KnownField d
--                  , Real (V.Snd d)
                  )
              => (Int -> Double -> V.Snd d) -> Int -> UVec.Vector Double -> [F.Record (ks V.++ '[d])]
vecDemoAsRecs toD pop vecDemo =
  let keyRecs = Set.toAscList $ Keyed.elements @(F.Record ks)
      values :: [Double] = UVec.toList vecDemo
      makeD :: Double -> F.Record '[d]
      makeD x = toD pop x F.&: V.RNil
  in zipWith (\k v -> k F.<+> (makeD v)) keyRecs values 

districtAsRecs :: forall ks d as rs.
                  (Ord (F.Record ks)
                  , ks F.⊆ rs
                  , Keyed.FiniteSet (F.Record ks)
                  , F.ElemOf rs d
                  , V.KnownField d
                  , Real (V.Snd d)
                  , rs ~ (ks V.++ '[d])
                  )
               => (Int -> Double -> V.Snd d) -> DistrictP as rs -> [F.Record (as V.++ rs)]
districtAsRecs toD (DistrictP id _ pop vec) = fmap (\r -> id F.<+> r) $ vecDemoAsRecs @ks @d toD pop vec 
      

absMetric :: UVec.Vector Double -> UVec.Vector Double -> Double
absMetric xs ys =  UVec.sum $ UVec.map abs $ UVec.zipWith (-) xs ys

--meanAbsMetric :: UVec.Vector Double -> UVec.Vector Double -> Double
--meanAbsMetric xs ys =  FL.fold FL.mean  $ fmap abs $ Vec.zipWith (-) xs ys

euclideanMetric :: UVec.Vector Double -> UVec.Vector Double -> Double
euclideanMetric xs ys = UVec.sum $ UVec.map (\x -> x * x) $ UVec.zipWith (-) xs ys

districtDiff
  :: forall ks d as rs.
     (UVec.Vector Double -> UVec.Vector Double -> Double) 
  -> DistrictP as rs
  -> DistrictP as rs
  -> Double
districtDiff metric d1 d2 = metric (districtVec d1) (districtVec d2)

districtMakeSimilar :: forall ks d as.(Ord (F.Record ks)
                                      , ks F.⊆ (ks V.++ '[d])
                                      , F.ElemOf (ks V.++ '[d]) d
                                      , V.KnownField d
                                      , Real (V.Snd d)
                                      )
                    => (Double -> V.Snd d)
                    -> DistrictP as (ks V.++ '[d])
                    -> Double
                    -> DistrictP as (ks V.++ '[d])
                    -> DistrictP as (ks V.++ '[d])
districtMakeSimilar toD tgtD x patD =
  let f t p = p * (1 - x) + t * x
      newPatVec = UVec.zipWith f (districtVec tgtD) (districtVec patD)
      newPatPop = round $ f (realToFrac $ districtPop tgtD) (realToFrac $ districtPop patD) 
  in DistrictP (districtId patD) True newPatPop newPatVec

buildSOM :: forall ks d as r.
  (K.KnitEffects r
  , K.Member PRF.RandomFu r
  , ks F.⊆ (ks V.++ '[d])
  , V.KnownField d
  , F.ElemOf (ks V.++ '[d]) d
  , Real (V.Snd d)
  , Ord (F.Record ks)
  )
  => (UVec.Vector Double -> UVec.Vector Double -> Double) -- ^ metric for SOM
  -> (Int, Int) -- rectangular grid sizes
  -> [DistrictP as (ks V.++ '[d])]
  -> K.Sem r (SOM.SOM Double Double (GridMap.LGridMap Grid.RectSquareGrid) Double (Int, Int) (DistrictP as (ks V.++ '[d])))
buildSOM metric (gridRows, gridCols) sampleFrom = do
  let g = Grid.rectSquareGrid gridRows gridCols
      numSamples = gridRows * gridCols
      sampleUniqueIndex is = do
        newIndex <- PRF.sampleRVar (RandomFu.uniform 0 (length sampleFrom - 1))
        if newIndex `elem` is then sampleUniqueIndex is else return (newIndex : is)
  sampleIndices <- FL.foldM (FL.FoldM (\is _ -> sampleUniqueIndex is) (return []) return) [1..numSamples]
  let samples = fmap (\n -> sampleFrom !! n) sampleIndices
      gm = GridMap.lazyGridMap g samples
      n' = fromIntegral (length sampleFrom)
      lrf = SOM.decayingGaussian 0.5 0.1 0.3 0.1 n'
      dDiff = districtDiff @ks @d metric
      dMakeSimilar = districtMakeSimilar @ks @d (fromIntegral . round)
  return $ SOM.SOM gm lrf dDiff dMakeSimilar 0


-- NB: Weight is chosen so that for exactly the same and equidistributed,
-- tr (M'M) = (numPats * numPats)/numClusters
sameClusterMatrixSOM :: forall c v k p dk. (SOM.Classifier c v k p, Ord dk, Eq k, Ord v) => (p -> dk) -> c v k p -> [p] -> SLA.SpMatrix Int
sameClusterMatrixSOM getKey som ps = sameClusterMatrix getKey (SOM.classify som) ps

sameClusterMatrix :: forall k p dk. (Ord dk, Eq k) => (p -> dk) -> (p -> k) -> [p] -> SLA.SpMatrix Int
sameClusterMatrix getKey cluster ps =
  let numPats = length ps
      getCluster :: p -> State.State (M.Map dk k) k
      getCluster p = do
        let k = getKey p
        m <- State.get
        case M.lookup k m of
          Just c -> return c
          Nothing -> do
            let c = cluster p
            State.put (M.insert k c m)
            return c      
      go :: (Int, [p]) -> State.State (M.Map dk k) [(Int, Int, Int)]
      go (n, ps) = do
        ks <- traverse getCluster ps
        let f k1 (k2, m) = if k1 == k2 then Just (n, m, 1) else Nothing
            swapIndices (a, b, h) = (b, a, h) 
        case ks of
          [] -> return []
          (kh : kt) -> do
            let upper = catMaybes (fmap (f kh) $ zip kt [(n+1)..])
                lower = fmap swapIndices upper
            return $ (n,n,1) : (upper ++ lower)
      scM = List.concat <$> (traverse go (zip [0..] $ List.tails ps))
  in SLA.fromListSM (numPats, numPats) $ State.evalState scM M.empty


randomSCM :: K.Member GLM.RandomFu r => Int -> Int -> K.Sem r (SLA.SpMatrix Int)
randomSCM nPats nClusters = do
  clustered <- traverse (const $ PRF.sampleRVar (RandomFu.uniform 1 nClusters)) [1..nPats]
  let go :: (Int, [Int]) -> [(Int, Int, Int)]
      go (n, ks) =
        let f k1 (k2, m) = if k1 == k2 then Just (n, m, 1) else Nothing
            swapIndices (a, b, h) = (b, a, h)
        in case ks of
          [] -> []
          (kh : kt) ->
            let upper = catMaybes (fmap (f kh) $ zip kt [(n+1)..])
                lower = fmap swapIndices upper
            in (n,n,1) : (upper ++ lower)
      scM = List.concat $ fmap go $ zip [0..] $ List.tails clustered
  return $ SLA.fromListSM (nPats, nPats) scM

type Method = "Method" F.:-> T.Text
type Mean = "mean" F.:-> Double
type Sigma = "sigma" F.:-> Double
type ScaledDelta = "scaled_delta" F.:-> Double
type FlipIndex = "flip_index" F.:-> Double

computeScaledDelta
  :: forall ks a f r. 
  (K.KnitEffects r
  , Foldable f
  , FI.RecVec (ks V.++ [Method, Mean, Sigma, ScaledDelta, FlipIndex])
  )
  => T.Text  
  -> (a -> a -> Double)
  -> Double 
  -> (a -> Double)
  -> (a -> F.Record ks)
  -> f a
  -> K.Sem r (F.FrameRec (ks V.++ [Method, Mean, Sigma, ScaledDelta, FlipIndex]))
computeScaledDelta methodName distance perplexity qty key rows = do
  let length = FL.fold FL.length rows
      datV = MA.fromList @MA.B MA.Seq $ FL.fold FL.list rows
      xs = MA.map qty datV
      rawDistances
        = MA.computeAs MA.U $ TSNE.symmetric MA.Seq (MA.Sz1 length)
          $ \(i MA.:. j) -> distance (datV MA.! i) (datV MA.! j)
      meanDistance = MA.sum rawDistances / (realToFrac $ length * length)
      distances = MA.computeAs MA.U $ MA.map (/meanDistance) rawDistances 
  K.logLE K.Info "Computing probabilities from distances.."
--  K.logLE K.Info $ T.pack $ show distances
  probabilities <- K.knitEither
                   $ either (Left . T.pack . show) Right
                   $ Perplexity.probabilities perplexity distances
--  K.logLE K.Info $ T.pack $ show probabilities
  K.logLE K.Info "Finished computing probabilities from distances."
  let dist v =
        let xw = MA.computeAs MA.B $ MA.zip xs v
            mean = FL.fold FLS.meanWeighted xw
            var = FL.fold (FLS.varianceWeighted mean) xw
        in (mean, var)            
      dists = MA.map dist (MA.outerSlices probabilities)
      asRec :: a -> (Double, Double) -> F.Record (ks V.++ [Method, Mean, Sigma, ScaledDelta, FlipIndex])
      asRec a (m, v) =
        let sigma = sqrt v
            scaledDelta = (qty a - m)/sigma
            flipIndex = if (qty a - 0.5) * (m - 0.5) < 0 then abs scaledDelta else 0
            suffixRec :: F.Record [Method, Mean, Sigma, ScaledDelta, FlipIndex]
            suffixRec = methodName F.&: m F.&: sigma F.&: scaledDelta F.&: flipIndex F.&: V.RNil
        in key a F.<+> suffixRec
  return $ F.toFrame $ MA.computeAs MA.B $ MA.zipWith asRec datV dists
  
post :: forall r.(K.KnitMany r, K.CacheEffectsD r, K.Member GLM.RandomFu r) => Bool -> K.Sem r ()
post updated = P.mapError BR.glmErrorToPandocError $ K.wrapPrefix "DistrictClustering" $ do

  let clusterRowsToS (cs, rs) = (fmap FS.toS cs, fmap FS.toS rs)
      clusterRowsFromS (cs', rs') = (fmap FS.fromS cs', fmap FS.fromS rs')

  pums2018ByCD_C <- do
    demo_C <- PUMS.pumsLoader Nothing
    cd116FromPUMA2012_C <- BR.cd116FromPUMA2012Loader
    let cachedDeps = (,) <$> demo_C <*> cd116FromPUMA2012_C
    BR.retrieveOrMakeFrame "mrp/DistrictClusters/pums2018ByCD.bin" cachedDeps $ \(pumsRaw, cdFromPUMA) -> do
      pumsCDRollup <- PUMS.pumsCDRollup ((== 2018) . F.rgetField @BR.Year) (PUMS.pumsKeysToASER5 True . F.rcast) cdFromPUMA pumsRaw
      return pumsCDRollup

  houseVoteShare_C <- do
    houseResults_C <- BR.houseElectionsLoader
    BR.retrieveOrMakeFrame "mrp/DistrictClusters/houseVoteShare.bin" houseResults_C $ \houseResultsRaw -> do
      let filterHR r = F.rgetField @BR.Year r `elem` [2016, 2018]
                       && F.rgetField @BR.Stage r == "gen"
                       && F.rgetField @BR.Runoff r == False
                       && F.rgetField @BR.Special r == False
                       && (F.rgetField @ET.Party r == ET.Democratic || F.rgetField @ET.Party r == ET.Republican)
          houseResults = fmap (F.rcast @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, ET.Party, ET.Votes, ET.TotalVotes])
                         $ F.filterFrame filterHR houseResultsRaw
          houseResultsF = FMR.concatFold $ FMR.mapReduceFold
                              FMR.noUnpack
                              (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict])
                              (FMR.foldAndAddKey votesToVoteShareF)
      return $ FL.fold houseResultsF houseResults

--  let voteShare2016 :: F.FrameRec BR.HouseElectionCols
--                    -> F.FrameRec [BR.StateAbbreviation, BR.CongressionalDistrict, Pref2016]
  let voteShare2016 hvs = fmap (FT.replaceColumn @ET.DemPref @Pref2016 id . F.rcast @[BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref])
                          $ F.filterFrame ((==2016) . F.rgetField @BR.Year) hvs


  pumsByCDWithVoteShare_C <- do
    let cachedDeps = (,) <$> pums2018ByCD_C <*> houseVoteShare_C
    BR.retrieveOrMakeFrame "mrp/DistrictClusters/pumsByCDWithVoteShare.bin" cachedDeps $ \(pumsByCD, houseVoteShare) -> do
      K.logLE K.Info $ "Joining demographics with vote share data"
      let (pumsByCDWithVoteShare, missing) = FJ.leftJoinWithMissing  @([BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict])
                                             pumsByCD
                                             houseVoteShare
      K.logLE K.Info $ "Districts with missing house results (dropped from analysis):" <> (T.pack $ show $ List.nub $ missing)
      return pumsByCDWithVoteShare

  K.logLE K.Info $ "Building clustering input"
  districtsForClustering :: [DistrictP [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PUMS.Citizens] (DT.CatColsASER5 V.++ '[PUMS.Citizens])] <- do
    pumsByCDWithVoteShare <- K.ignoreCacheTime pumsByCDWithVoteShare_C
    let asVec rs = UVec.fromList
                   $ fmap snd
                   $ M.toAscList
                   $ FL.fold (FL.premap (\r-> (F.rcast @DT.CatColsASER5 r, realToFrac $ F.rgetField @PUMS.Citizens r)) FL.map) rs
        pop rs = FL.fold (FL.premap (F.rgetField @PUMS.Citizens) FL.sum) rs
        popRec :: Int -> F.Record '[PUMS.Citizens] = \n -> n F.&: V.RNil
    return
      $ FL.fold
      (FMR.mapReduceFold
       FMR.noUnpack
       (FMR.assignKeysAndData @[BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref] @(DT.CatColsASER5 V.++ '[PUMS.Citizens]))
       (FMR.ReduceFold $ \k -> fmap (\rs -> DistrictP (k F.<+> popRec (pop rs)) False (pop rs) (asVec rs)) FL.list)
      )
      pumsByCDWithVoteShare
  K.logLE K.Info $ "After joining with 2018 voteshare info, working with " <> (T.pack $ show $ length districtsForClustering) <> " districts."


  K.logLE K.Info "Computing scaled deltas for raw districts."
  rawSD <- do
    let rawDist = districtDiff euclideanMetric
    computeScaledDelta "Raw" rawDist 40.0 (F.rgetField @ET.DemPref . districtId) districtId $ districtsForClustering
  logFrame rawSD
    
  let districtsForClustering_C = fmap (const districtsForClustering) pumsByCDWithVoteShare_C
  K.logLE K.Info $ "Computing top 2 PCA components"
  (pca1v, pca2v) <- do
    let center v = let m = (realToFrac $ GVec.sum v)/(realToFrac $ GVec.length v) in GVec.map ((-) m) v
        asSVecs = fmap (center . SVec.convert . districtVec) districtsForClustering
        asMat = LA.fromRows asSVecs
        mTm = (LA.tr asMat) LA.<> asMat
        (eigVals, eigVecs) = LA.eigSH (LA.trustSym mTm)
        pc1Vec = SVec.convert $ (LA.toColumns eigVecs) List.!! 0
        pc2Vec = SVec.convert $ (LA.toColumns eigVecs) List.!! 1
    K.logLE K.Diagnostic $ "Eigenvalues: " <> (T.pack $ show eigVals)
    K.logLE K.Diagnostic $ "PC 1:" <> (T.pack $ show (vecDemoAsRecs @DT.CatColsASER5 @PUMS.Citizens (\pop pct -> round $ pct * realToFrac pop) 100 $ pc1Vec))
    K.logLE K.Diagnostic $ "PC 2:" <> (T.pack $ show (vecDemoAsRecs @DT.CatColsASER5 @PUMS.Citizens (\pop pct -> round $ pct * realToFrac pop) 100 $ pc2Vec))
    return (pc1Vec, pc2Vec)

  let numComps = 3
      pairsWithFirst l = case l of
        [] -> []
        (x : []) -> []
        (x : xs) -> fmap (x,) xs
      allDiffPairs = List.concat . fmap pairsWithFirst . List.tails

  let kClusterDistricts = do
        K.logLE K.Info $ "Computing K-means"
        let labelCD r = F.rgetField @BR.StateAbbreviation r <> "-" <> (T.pack $ show $ F.rgetField @BR.CongressionalDistrict r)            
            distWeighted :: MK.Weighted (DistrictP [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PUMS.Citizens] (DT.CatColsASER5 V.++ '[PUMS.Citizens])) Double
            distWeighted = MK.Weighted
                           40
                           districtVec
                           (realToFrac . districtPop)
            metric = euclideanMetric
        initialCentroids <- PMR.absorbMonadRandom $ MK.kMeansPPCentroids metric 5 (fmap districtVec districtsForClustering)
        (MK.Clusters kmClusters, iters) <- MK.weightedKMeans (MK.Centroids $ Vec.fromList initialCentroids) distWeighted metric districtsForClustering
        let distRec :: DistrictP [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PUMS.Citizens] (DT.CatColsASER5 V.++ '[PUMS.Citizens])
                    -> F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PUMS.Citizens, W, PC1, PC2]
            distRec d =
              let pca1 = UVec.sum $ UVec.zipWith (*) pca1v (districtVec d)
                  pca2 = UVec.sum $ UVec.zipWith (*) pca2v (districtVec d)
                  w = districtPop d
              in (districtId d) F.<+> (realToFrac w F.&: pca1 F.&: pca2 F.&: V.RNil)
            processOne c = do
              (centroidUVec, cW) <- MK.centroid distWeighted (MK.members c)
              let cPCA1 = UVec.sum $ UVec.zipWith (*) pca1v centroidUVec
                  cPCA2 = UVec.sum $ UVec.zipWith (*) pca2v centroidUVec
                  withProjs = fmap distRec (MK.members c)
              return $ ((cPCA1, cPCA2, cW), withProjs)
        rawClusters <- K.knitMaybe "Empty Cluster in K_means." $ traverse processOne  (Vec.toList kmClusters) 
        return $ FK.clusteredRowsFull @PC1 @PC2 @W labelCD $ M.fromList [((2018 F.&: V.RNil) :: F.Record '[BR.Year], rawClusters)]

  
  kClusteredDistricts <- kClusterDistricts
--  logFrame $ snd kClusteredDistricts
  K.logLE K.Info "Computing scaled deltas for top two PCA embedding."
  pcaSD   <- do
    let pcaXY r = (F.rgetField @PC1 r, F.rgetField @PC2 r)    
        pcaDist r1 r2 =
          let (x1, y1) = pcaXY r1
              (x2, y2) = pcaXY r2
              dx = x1 - x2
              dy = y1 - y2
          in (dx * dx) + (dy * dy)
    computeScaledDelta "PCA" pcaDist 40.0 (F.rgetField @ET.DemPref) id $ snd kClusteredDistricts

  K.logLE K.Info $ "K-means diagnostics"
  let kClustered = snd kClusteredDistricts
      numKDists = length kClustered 
      kSCM = sameClusterMatrix (F.rcast @'[FK.MarkLabel]) (F.rcast @'[FK.ClusterId]) kClustered
      kEffClusters = realToFrac (numKDists * numKDists)/ (realToFrac $ SLA.trace $ SLA.matMat_ SLA.ABt kSCM kSCM)
      kCompFactor = kEffClusters / realToFrac (numKDists * numKDists)
  K.logLE K.Info $ "Effective clusters=" <> (T.pack $ show kEffClusters)  
  K.logLE K.Info $ "Making " <> (T.pack $ show numComps) <> " other k-means clusterings for comparison..."
  kComps <- fmap snd <$> traverse (const kClusterDistricts) [1..numComps]
  let kSCMs = fmap  (sameClusterMatrix (F.rcast @'[FK.MarkLabel]) (F.rcast @'[FK.ClusterId])) kComps
      allKSCMPairs = allDiffPairs kSCMs
      allDiffKTraces = fmap (\(scm1, scm2) -> kCompFactor * (realToFrac $ SLA.trace $ SLA.matMat_ SLA.ABt scm1 scm2)) allKSCMPairs
      (meanKC, stdKC) = FL.fold ((,) <$> FL.mean <*> FL.std) allDiffKTraces
  K.logLE K.Info $ "K-meansL <comp> = " <> (T.pack $ show meanKC) <> "; sigma(comp)=" <> (T.pack $ show stdKC)

-- tSNE
  K.logLE K.Info $ "tSNE embedding..."    
  let tsnePerplexity = 10
      tsneIters = [1000]
      tsnePerplexities = [tsnePerplexity]
      tsneLearningRates = [10]
      tsneClusters = 5
--  K.clearIfPresent "mrp/DistrictClusters/tsne.bin"
  (tSNE_ClustersF, tSNE_ClusteredF) <- K.ignoreCacheTimeM
            $ BR.retrieveOrMake2Frames "mrp/DistrictClusters/tsne.bin" districtsForClustering_C $ \dfc -> do
    K.logLE K.Info $ "Running tSNE gradient descent for " <> (T.pack $ show tsneIters) <> " iterations."    
    tsneMs <- BR.runTSNE
              (Just 1)
              districtId
              (UVec.toList . districtVec)
              tsnePerplexities
              tsneLearningRates
              tsneIters
              (\x -> (BR.tsneIteration2D_M x, BR.tsneCost2D_M x, BR.solutionToList $ BR.tsneSolution2D_M x))
              BR.tsne2D_S
              dfc
           
    let tSNERec :: BR.TSNEParams -> (Double, Double) -> F.Record [TSNEPerplexity, TSNELearningRate, TSNEIters, TSNE1, TSNE2] 
        tSNERec (BR.TSNEParams p lr n) (x, y) = p F.&: lr F.&: n F.&: x F.&: y F.&: V.RNil
        tSNERecs p = fmap (\(k, tSNEXY) -> k F.<+> tSNERec p tSNEXY) . M.toList
        fullTSNEResult =  mconcat $ fmap (\(p, solM) -> F.toFrame $ tSNERecs p solM) $ tsneMs

    -- do k-means clustering on TSNE
        labelCD r = F.rgetField @BR.StateAbbreviation r <> "-" <> (T.pack $ show $ F.rgetField @BR.CongressionalDistrict r)
        getVec r = UVec.fromList $ [F.rgetField @TSNE1 r, F.rgetField @TSNE2 r]
        distWeighted :: MK.Weighted
                        (F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PUMS.Citizens, TSNEPerplexity, TSNELearningRate, TSNEIters, TSNE1, TSNE2]) Double
        distWeighted = MK.Weighted
                       2
                       getVec
                       (realToFrac . F.rgetField @PUMS.Citizens)
        metric = euclideanMetric
    initialCentroids <- PMR.absorbMonadRandom $ MK.kMeansPPCentroids metric tsneClusters (fmap getVec $ FL.fold FL.list fullTSNEResult)
    (MK.Clusters kmClusters, iters) <- MK.weightedKMeans (MK.Centroids $ Vec.fromList initialCentroids) distWeighted metric fullTSNEResult
    let processOne c = do
          (centroidUVec, cW) <- MK.centroid distWeighted (MK.members c)
          let wRec :: Double -> F.Record '[W] = \x -> x F.&: V.RNil
              addW r = r F.<+> wRec (realToFrac $ F.rgetField @PUMS.Citizens r)
              withWeights = fmap addW $ MK.members c
          return ((centroidUVec UVec.! 0, centroidUVec UVec.! 1, cW), withWeights)
    rawClusters <- K.knitMaybe "Empty Cluster in tSNE K-means." $ traverse processOne (Vec.toList kmClusters)
    let (clustersL, clusteredDistrictsL)
          = FK.clusteredRowsFull @TSNE1 @TSNE2 @W labelCD $ M.fromList [((2018 F.&: V.RNil) :: F.Record '[BR.Year], rawClusters)]
    K.logLE K.Info "tSNE computation done."
    return (F.toFrame clustersL, F.toFrame clusteredDistrictsL)

  K.logLE K.Info "Computing scaled deltas for tSNE result."
  tSNE_ClusteredWithSD <- do
    let tsneXY r = (F.rgetField @TSNE1 r, F.rgetField @TSNE2 r)
        tsneDistance r1 r2 =
          let (x1, y1) = tsneXY r1
              (x2, y2) = tsneXY r2
              dX = x1 - x2
              dY = y1 - y2
          in (dX * dX) + (dY * dY)
    computeScaledDelta "tSNE" tsneDistance (realToFrac tsnePerplexity) (F.rgetField @ET.DemPref) id tSNE_ClusteredF
    
  --  logFrame tSNE_ClusteredWithSD
  -- build a frame of demographic buckets with tSNE info attached
  let longDistrictData = F.toFrame
                         $ concat
                         $ fmap (\d -> fmap (districtId d F.<+>) (vecDemoAsRecs @DT.CatColsASER5 @PopPct (\tp p -> 100*p / realToFrac tp) (round $ UVec.sum $ districtVec d) (districtVec d))) districtsForClustering
  longDistrictsWith_tSNE <- K.knitEither
                            $ either (Left . T.pack . show) Right
                            $ FJ.leftJoinE @[BR.StateAbbreviation, BR.CongressionalDistrict] longDistrictData tSNE_ClusteredWithSD
--  logFrame longDistrictsWith_tSNE

-- SOM      
  K.logLE K.Info $ "SOM clustering..."
  let gridRows = 3
      gridCols = 3
      numDists = length districtsForClustering
      metric = euclideanMetric
      somResultsKey = "mrp/DistrictClusters/SOM_Results.bin"
--  K.clearIfPresent somResultsKey
  (districtsWithStatsF, districtsOnMap) <- K.ignoreCacheTimeM $ do
    retrieveOrMake2Frames somResultsKey districtsForClustering_C $ \dfc -> do
      randomOrderDistricts <- PRF.sampleRVar $ RandomFu.shuffle dfc
      som0 <- buildSOM @DT.CatColsASER5 @PUMS.Citizens metric (gridRows, gridCols) dfc
      let som = SOM.trainBatch som0 randomOrderDistricts  
      K.logLE K.Info $ "SOM Diagnostics"

      let scm = sameClusterMatrixSOM districtId som districtsForClustering
          effClusters = realToFrac (numDists * numDists)/ (realToFrac $ SLA.trace $ SLA.matMat_ SLA.ABt scm scm)
      K.logLE K.Info $ "Effective clusters=" <> (T.pack $ show effClusters)  
      let compFactor = effClusters / realToFrac (numDists * numDists)
      K.logLE K.Info $ "Making " <> (T.pack $ show numComps) <> " different SOMs"
      som0s <- traverse (const $ buildSOM @DT.CatColsASER5 @PUMS.Citizens metric (gridRows, gridCols) districtsForClustering) [1..numComps]
      let soms = fmap (\s -> SOM.trainBatch s randomOrderDistricts) som0s
          scms = fmap (\s -> sameClusterMatrixSOM districtId s districtsForClustering) soms

          allDiffSCMPairs = allDiffPairs scms
          allDiffTraces = fmap (\(scm1, scm2) -> compFactor * (realToFrac $ SLA.trace $ SLA.matMat_ SLA.ABt scm1 scm2)) allDiffSCMPairs
          (meanC, stdC) = FL.fold ((,) <$> FL.mean <*> FL.std) allDiffTraces
      K.logLE K.Info $ "<comp> = " <> (T.pack $ show meanC) <> "; sigma(comp)=" <> (T.pack $ show stdC)

      K.logLE K.Info $ "Generating random ones for comparison"
      randomSCMs <- traverse (const $ randomSCM numDists (gridRows * gridCols)) [1..numComps]
      let allRandomTraces = fmap (\scm ->  SLA.trace $ SLA.matMat_ SLA.ABt scm scm) randomSCMs
      let allSameEffClusters = fmap (\scm ->  realToFrac (numDists * numDists)/ (realToFrac $ SLA.trace $ SLA.matMat_ SLA.ABt scm scm)) randomSCMs
          (meanRCs, stdRCs) = FL.fold ((,) <$> FL.mean <*> FL.std) allSameEffClusters
      K.logLE K.Info $ "Random SCMs: <eff Clusters> = " <> (T.pack $ show meanRCs) <> "; sigma(eff Clusters)=" <> (T.pack $ show stdRCs)      
      let allDiffPairsRandom = allDiffPairs randomSCMs
          randomCompFactor = meanRCs / realToFrac (numDists * numDists)
          allRandomDiffTraces = fmap (\(scm1, scm2) -> randomCompFactor * (realToFrac $ SLA.trace $ SLA.matMat_ SLA.ABt scm1 scm2)) allDiffPairsRandom
          (meanRS, stdRS) = FL.fold ((,) <$> FL.mean <*> FL.std) allRandomDiffTraces
      K.logLE K.Info $ "Random SCs: <comp> = " <> (T.pack $ show meanRS) <> "; sigma(comp)=" <> (T.pack $ show stdRS)

      K.logLE K.Info $ "Building SOM heatmap of DemPref"    
      let districtPref = F.rgetField @ET.DemPref . districtId
          heatMap0 :: GridMap.LGridMap Grid.RectSquareGrid (Int, Double)
          heatMap0  = GridMap.lazyGridMap (Grid.rectSquareGrid gridRows gridCols) $ replicate (gridRows * gridCols) (0, 0)
          adjustOne pref (districts, avgPref)  = (districts + 1, ((realToFrac districts * avgPref) + pref)/(realToFrac $ districts + 1))
          heatMap = FL.fold (FL.Fold (\gm d -> let pref = districtPref d in GridMap.adjust (adjustOne pref) (SOM.classify som d) gm) heatMap0 id) districtsForClustering
      K.logLE K.Info $ "Building cluster table"
      let distStats :: DistrictP [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PUMS.Citizens] (DT.CatColsASER5 V.++ '[PUMS.Citizens])
                    -> F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PUMS.Citizens, PctWWC, PctNonWhite, PctYoung, SOM_Cluster]
          distStats d =
            let vac = F.rgetField @PUMS.Citizens
                vacF = fmap realToFrac $ FL.premap vac FL.sum
                pctWWCF = (/) <$> FL.prefilter isWWC vacF <*> vacF
                pctNonWhite = (/) <$> FL.prefilter isNonWhite vacF <*> vacF
                pctYoung = (/) <$> FL.prefilter isYoung vacF <*> vacF
                statsRec :: [F.Record (DT.CatColsASER5 V.++ '[PUMS.Citizens])] -> F.Record [PctWWC, PctNonWhite, PctYoung]
                statsRec rs = FL.fold ((\x y z -> x F.&: y F.&: z F.&: V.RNil) <$> pctWWCF <*> pctNonWhite <*> pctYoung) rs
                clusterRec :: F.Record '[SOM_Cluster] = SOM.classify som d F.&: V.RNil
            in districtId d F.<+> statsRec (fmap F.rcast $ districtAsRecs @DT.CatColsASER5 @PUMS.Citizens (\pop pct -> round $ pct * realToFrac pop) d) F.<+> clusterRec
      districtsWithStatsF :: F.FrameRec DWSRow <- do
        hvs <- K.ignoreCacheTime houseVoteShare_C 
        let vs2016 = voteShare2016 hvs
            (districtsWithStats, missing2016Share)  = FJ.leftJoinWithMissing @[BR.StateAbbreviation, BR.CongressionalDistrict]
                                                      (F.toFrame $ fmap distStats districtsForClustering)
                                                      vs2016
        K.logLE K.Info $ "Districts with missing 2016 house results (dropped from analysis):" <> (T.pack $ show $ List.nub $ missing2016Share)
        return $ F.toFrame districtsWithStats
-- logFrame districtsWithStatsF
      
  
--  K.liftKnit $ F.writeCSV "districtStats.csv" withStatsF
      K.logLE K.Info $ "Building neighbor plot for SOM"
      let gm = SOM.toGridMap som
          bmus :: forall k p x t d gm. (Grid.Index (gm p) ~ k
                                       , Grid.Index (GridMap.BaseGrid gm p) ~ k
                                       , Grid.Index (GridMap.BaseGrid gm x) ~ k
                                       , GridMap.GridMap gm p
                                       , GridMap.GridMap gm x
                                       , Grid.Grid (gm p)
                                       , Num t
                                       , Num x
                                       , Num d
                                       , Ord x
                                       , Ord k                                   
                                       ) => (p -> p -> x) -> SOM.SOM t d gm x k p -> p -> Maybe [(k, x)]
          bmus diff som p = do
            let gm = SOM.toGridMap som
                k1 = SOM.classify som p
            n1 <- GridMap.lookup k1 gm
            let neighbors = Grid.neighbours (GridMap.toGrid gm) k1
                neighborIndexAndDiff :: k -> Maybe (k, x)
                neighborIndexAndDiff k = do
                  nn <- GridMap.lookup k gm
                  return $ (k, diff p nn)
            neighborDiffs <- traverse neighborIndexAndDiff neighbors
            return $ (k1, diff n1 p) : neighborDiffs
          coords :: [((Int, Int), Double)] -> (Double, Double)
          coords ns =
            let bmu = head ns
                bmuD = snd bmu
                wgt = snd
                sumWgtF = FL.premap (\(_,d) -> 1/d) FL.sum
                avgXF = (/) <$> (FL.premap (\((x,_),d) -> realToFrac x/d) FL.sum) <*> sumWgtF
                avgYF = (/) <$> (FL.premap (\((_,y),d) -> realToFrac y/d) FL.sum) <*> sumWgtF
            in case bmuD of
              0 -> (\(x, y) -> (realToFrac x, realToFrac y)) $ fst bmu          
              _ -> FL.fold ((,) <$> avgXF <*> avgYF) ns
          districtSOMCoords :: DistrictP [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PUMS.Citizens] (DT.CatColsASER5 V.++ '[PUMS.Citizens])
                            -> Maybe (F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PUMS.Citizens, SOM_Cluster, '("X",Double), '("Y", Double)])
          districtSOMCoords d = do
            let dDiff = districtDiff @DT.CatColsASER5 @PUMS.Citizens metric        
            (c, (x, y)) <- do
              districtBMUs <- bmus dDiff som d
              let (dX, dY) =  coords districtBMUs
                  dC = fst $ head districtBMUs
              return (dC, (dX, dY))
            let cxyRec :: F.Record [SOM_Cluster, '("X", Double), '("Y", Double)] = c F.&: x F.&: y F.&: V.RNil
            return $ V.rappend (districtId d) cxyRec

      districtsOnMap <- F.toFrame <$> (K.knitMaybe "Error adding SOM coords to districts" $ traverse districtSOMCoords districtsForClustering)
      return (districtsWithStatsF, districtsOnMap)
--  logFrame districtsOnMap

  K.logLE K.Info "Computing scaled deltas for SOM embedding."
  somSD   <- do
    let somXY r = (F.rgetField @'("X", Double) r, F.rgetField @'("Y", Double) r)    
        somDist r1 r2 =
          let (x1, y1) = somXY r1
              (x2, y2) = somXY r2
              dx = x1 - x2
              dy = y1 - y2
          in (dx * dx) + (dy * dy)
    computeScaledDelta "SOM" somDist 40.0 (F.rgetField @ET.DemPref) id districtsOnMap


  -- put all scaled deltas together
  let scaledDeltaCols = F.rcast @[BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, Method, Mean, Sigma, ScaledDelta, FlipIndex]
      allScaledDeltas =  fmap scaledDeltaCols tSNE_ClusteredWithSD
                         <> fmap scaledDeltaCols pcaSD
                         <> fmap scaledDeltaCols rawSD
                         <> fmap scaledDeltaCols somSD
  hvs <- K.ignoreCacheTime houseVoteShare_C 
  let vs2016 = voteShare2016 hvs
  scaledDeltasW2016 <- K.knitEither
                       $ either (Left . T.pack . show) Right
                       $ FJ.leftJoinE @[BR.StateAbbreviation, BR.CongressionalDistrict]
                       allScaledDeltas
                       vs2016
  let allScaledDeltasForTableF =
        let methodsF :: FL.Fold (F.Record [Method, Mean, Sigma, ScaledDelta, FlipIndex]) (M.Map T.Text (F.Record [Mean, Sigma, ScaledDelta, FlipIndex]))
            methodsF = FL.premap (\r -> (F.rgetField @Method r, F.rcast r)) FL.map 
        in MR.mapReduceFold
           MR.noUnpack
           (MR.assign (F.rcast @[BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref]) (F.rcast @[Method, Mean, Sigma, ScaledDelta, FlipIndex]))
           (MR.foldAndLabel methodsF (,))
      sortFunction = fromMaybe 0 . fmap (F.rgetField @ScaledDelta) . M.lookup "Raw" . snd     
      allScaledDeltasForTable = List.sortOn sortFunction $ FL.fold allScaledDeltasForTableF allScaledDeltas
      
--  K.logLE K.Info $ "allScaledDeltas: " <> (T.pack $ show allScaledDeltasForTable)    
  curDate <-  (\(Time.UTCTime d _) -> d) <$> K.getCurrentTime
  let pubDateDistrictClusters =  Time.fromGregorian 2020 7 25
  K.newPandoc
    (K.PandocInfo ((postRoute PostDistrictClusters) <> "main")
      (brAddDates updated pubDateDistrictClusters curDate
       $ M.fromList [("pagetitle", "Looking for Flippable House Districts")
                    ,("title","Looking For Flippable House Districts")
                    ]
      ))
      $ do        
        brAddMarkDown textIntro
        brLineBreak
        brAddMarkDown textPCAPre
        _ <- K.addHvega Nothing Nothing
             $ clusterVL @PC1 @PC2
             "2018 House Districts: Demographic Principal Components"
             (FV.ViewConfig 800 800 10)
             (fmap F.rcast $ fst kClusteredDistricts)
             (fmap F.rcast $ snd kClusteredDistricts)
        brAddMarkDown textPCAPost
{-        
        _ <- K.addHvega Nothing Nothing
             $ kMeansBoxes @PCA1 @PCA2
             "2018 House Districts: K-Means"
             (FV.ViewConfig 800 800 10)
             (fmap F.rcast $ fst kClusteredDistricts)
             (fmap F.rcast $ snd kClusteredDistricts)
-}
        brLineBreak
        brAddMarkDown textSOMPre
        _ <- K.addHvega Nothing Nothing
             $ somRectHeatMap
             "District SOM Heat Map"
             (FV.ViewConfig 800 800 10)
             (fmap F.rcast districtsOnMap)
        brAddMarkDown textSOMPost
        brLineBreak
        brAddMarkDown text_tSNEPre             
        _ <- K.addHvega Nothing Nothing
             $ tsneVL
             ("tSNE Embedding (Perplexity=" <> (T.pack $ show tsnePerplexity) <> "; Colored by Dem Vote Share)")
             TSNE_VoteShare
             (FV.ViewConfig 800 800 10)
             (fmap F.rcast tSNE_ClusteredWithSD)
        brAddMarkDown textAnomaly
        _ <- K.addHvega Nothing Nothing
             $ tsneVL
             ("tSNE Embedding (Perplexity=" <> (T.pack $ show tsnePerplexity) <> "; Colored by Dem Vote Share, Sized by Anomaly)")
             TSNE_Anomaly
             (FV.ViewConfig 800 800 10)
             (fmap F.rcast tSNE_ClusteredWithSD)
        brAddMarkDown textFlip     
        _ <- K.addHvega Nothing Nothing
             $ tsneVL
             ("tSNE Embedding (Perplexity=" <> (T.pack $ show tsnePerplexity) <> "; Colored by Dem Vote Share, Sized by Flip Index)")
             TSNE_Flip
             (FV.ViewConfig 800 800 10)
             (fmap F.rcast tSNE_ClusteredWithSD)
        _ <- K.addHvega Nothing Nothing
             $ tsneDemoVL
             ("tSNE Embedding (Perplexity=" <> (T.pack $ show tsnePerplexity) <> "; Colored by Demographic groups.")
             (FV.ViewConfig 100 100 10)
             (fmap F.rcast longDistrictsWith_tSNE)                         
        brAddMarkDown textAnomalyVsShare
        _ <- K.addHvega Nothing Nothing
             $ anomalyVsShare
             "Raw anomaly vs 2018 vote share (color for 2016 vote share)"
             (FV.ViewConfig 800 800 10)
             (fmap F.rcast $ F.filterFrame ((== "Raw") . F.rgetField @Method) scaledDeltasW2016)
        brAddMarkDown textQuestions
        brAddMarkDown textEndForNow
        
{-             
        _ <- K.addHvega Nothing Nothing
             $ somBoxes
             "District SOM Box & Whisker"
             (FV.ViewConfig 800 800 10)
             (fmap F.rcast districtsOnMap)
-}
       
        BR.brAddRawHtmlTable
          ("Districts With Scaled Delta")
          (BHA.class_ "brTable")
          (sdCollonnade mempty)
          allScaledDeltasForTable
        BR.brAddRawHtmlTable
          ("Districts With Stats")
          (BHA.class_ "brTable")
          (dwsCollonnade mempty)
          districtsWithStatsF
          
        brAddMarkDown brReadMore

anomalyVsShare :: Foldable f
  => T.Text
  -> FV.ViewConfig
  -> f (F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, ScaledDelta, FlipIndex, Pref2016])
  -> GV.VegaLite
anomalyVsShare title vc rows=
  let dat = FV.recordsToVLData id FV.defaultParse rows      
      make2018Share = GV.calculateAs "datum.DemPref - 0.5" "2018 Dem Vote Share"
      make2016Share = GV.calculateAs "datum.Pref2016 - 0.5" "2016 Dem Vote Share"
      makeAnomaly = GV.calculateAs "datum.scaled_delta" "Anomaly"
      transform = (GV.transform . make2018Share . make2016Share . makeAnomaly) []
      encX = GV.position GV.X [GV.PName "2018 Dem Vote Share", GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [GV.PName "Anomaly", GV.PmType GV.Quantitative]
      encColor = GV.color [GV.MName "2016 Dem Vote Share", GV.MmType GV.Quantitative, GV.MScale [GV.SScheme "redblue" [], GV.SDomainMid 0]]
      enc = (GV.encoding . encX . encY . encColor) []
      mark = GV.mark GV.Point [GV.MTooltip GV.TTData]
  in FV.configuredVegaLite vc [FV.title title, enc, mark, transform, dat]

type DemoLabel = "Demographic" F.:-> T.Text

catToLabel :: F.Record DT.CatColsASER5 -> F.Record '[DemoLabel]
catToLabel r =
  let ageLabel = case F.rgetField @DT.SimpleAgeC r of
        DT.Under -> "U"
        DT.EqualOrOver -> "O"
      sexLabel = case F.rgetField @DT.SexC r of
        DT.Female -> "F"
        DT.Male -> "M"
      educationLabel = case F.rgetField @DT.CollegeGradC r of
        DT.Grad -> "G"
        DT.NonGrad -> "N"
      raceLabel = case F.rgetField @DT.Race5C r of
        DT.R5_Black -> "B"
        DT.R5_Latinx -> "L"
        DT.R5_Asian -> "A"
        DT.R5_WhiteNonLatinx -> "W"
        DT.R5_Other -> "O"
  in (ageLabel <> sexLabel <> educationLabel <> raceLabel) F.&: V.RNil

        
tsneDemoVL ::  (Foldable f, Functor f)
           => T.Text
           -> FV.ViewConfig
           -> f (F.Record ([BR.StateAbbreviation, BR.CongressionalDistrict, TSNE1, TSNE2, PopPct] V.++ DT.CatColsASER5))
           -> GV.VegaLite
tsneDemoVL title vc rows =  
  let label r = FT.transform catToLabel r 
      dat = FV.recordsToVLData id FV.defaultParse (fmap label rows)
      makeDistrict = GV.calculateAs "datum.state_abbreviation + \"-\" + datum.congressional_district" "District"
      encX = GV.position GV.X [FV.pName @TSNE1, GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [FV.pName @TSNE2, GV.PmType GV.Quantitative]
      encColor = GV.color [FV.mName @PopPct, GV.MmType GV.Quantitative]
      encAll = encX . encY . encColor 
      enc = (GV.encoding . encAll) []
      mark = GV.mark GV.Circle [GV.MTooltip GV.TTData]
      transform = (GV.transform . makeDistrict) []
      facet = GV.facetFlow [FV.fName @DemoLabel, GV.FmType GV.Nominal]
      eachSpec = GV.asSpec [enc, transform, mark]
      resolve = GV.resolve . GV.resolution (GV.RScale [(GV.ChColor, GV.Independent)])
  in FV.configuredVegaLite vc [FV.title title, GV.specification (eachSpec), facet, resolve [], GV.columns 3, dat]

data TSNE_Chart = TSNE_VoteShare | TSNE_Anomaly | TSNE_Flip

tsneVL ::  Foldable f
       => T.Text
       -> TSNE_Chart
       -> FV.ViewConfig
       -> f (F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, FK.ClusterId, ScaledDelta, FlipIndex, TSNEPerplexity, TSNELearningRate, TSNEIters, TSNE1, TSNE2])
       -> GV.VegaLite
tsneVL title chartType vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows
      listF getField = fmap List.nub $ FL.premap getField FL.list
      (perpL, lrL, iterL) = FL.fold ((,,)
                            <$> listF (F.rgetField @TSNEPerplexity)
                            <*> listF (F.rgetField @TSNELearningRate)
                            <*> listF (F.rgetField @TSNEIters)) rows
      makeShare = GV.calculateAs "datum.DemPref - 0.5" "Dem Vote Share"
      makeAbsAnomaly = GV.calculateAs "abs (datum.scaled_delta)" "Anomaly Index"
      makeFlipIndex = GV.calculateAs "max(0.5, datum.flip_index)" "Flip Index"
      makeDistrict = GV.calculateAs "datum.state_abbreviation + \"-\" + datum.congressional_district" "District"
      encX = GV.position GV.X [FV.pName @TSNE1, GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [FV.pName @TSNE2, GV.PmType GV.Quantitative]
      encColor = GV.color [GV.MName "Dem Vote Share", GV.MmType GV.Quantitative, GV.MScale [GV.SScheme "redblue" [], GV.SDomainMid 0]]
      encFill = GV.fill [GV.MName "Dem Vote Share", GV.MmType GV.Quantitative, GV.MScale [GV.SScheme "redblue" [], GV.SDomainMid 0]]
      encAll = encX . encY . encColor . encFill
      encSpecific = case chartType of
        TSNE_VoteShare -> id
        TSNE_Anomaly -> GV.size [GV.MName "Anomaly Index", GV.MmType GV.Quantitative]
        TSNE_Flip -> GV.size [GV.MName "Flip Index", GV.MmType GV.Quantitative]
      enc = (GV.encoding . encAll . encSpecific) []
      mark = GV.mark GV.Point [GV.MTooltip GV.TTData]
      bindScales =  GV.select "scalesD" GV.Interval [GV.BindScales, GV.Clear "click[event.shiftKey]"]
      transform = (GV.transform . makeShare . makeDistrict . makeAbsAnomaly . makeFlipIndex) []
      selection = (GV.selection . bindScales) []
      resolve = (GV.resolve . GV.resolution (GV.RScale [ (GV.ChY, GV.Independent), (GV.ChX, GV.Independent)])) []
  in FV.configuredVegaLite vc [FV.title title, enc, mark, transform, selection, dat]
{-
      encCol = GV.column [FV.fName @TSNEIters, GV.FmType GV.Ordinal]
      encRow = GV.row [FV.fName @TSNEPerplexity, GV.FmType GV.Ordinal]
-}
  
somRectHeatMap :: (Foldable f, Functor f)
               => T.Text
               -> FV.ViewConfig
               -> f (F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, SOM_Cluster, '("X",Double), '("Y", Double)])
               -> GV.VegaLite
somRectHeatMap title vc distRows =
  let (nRows, nCols) = FL.fold ((,)
                                 <$> (fmap (fromMaybe 0) (FL.premap (fst . F.rgetField @SOM_Cluster) FL.maximum))
                                 <*> (fmap (fromMaybe 0) (FL.premap (snd . F.rgetField @SOM_Cluster) FL.maximum))) distRows
      distDat = FV.recordsToVLData id FV.defaultParse
                $ fmap (F.rcast @[BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, '("X",Double), '("Y", Double)]) distRows
      makeLabel = GV.calculateAs "datum.state_abbreviation + \"-\" + datum.congressional_district" "District"
      makeToggle = GV.calculateAs "\"Off\"" "Labels"
      prefToShare = GV.calculateAs "datum.DemPref - 0.5" "D Vote Share"
      encDX = GV.position GV.X [GV.PName "X", GV.PmType GV.Quantitative, GV.PAxis [GV.AxValues $ GV.Numbers $  fmap realToFrac [0..nRows]]]
      encDY = GV.position GV.Y [GV.PName "Y", GV.PmType GV.Quantitative, GV.PAxis [GV.AxValues $ GV.Numbers $  fmap realToFrac [0..nCols]]]
      encDColor = GV.color [GV.MName "D Vote Share", GV.MmType GV.Quantitative, GV.MScale [GV.SScheme "redblue" []]]
      encD = (GV.encoding . encDX . encDY . encDColor) []
      markD = GV.mark GV.Circle [GV.MTooltip GV.TTData]
      selectionD = GV.selection
                . GV.select "scalesD" GV.Interval [GV.BindScales, GV.Clear "click[event.shiftKey]"]
      selectionL = GV.selection . GV.select "LabelsS" GV.Single [GV.Fields ["Labels"], GV.Bind [GV.ICheckbox "Labels" [GV.InName "District Labels"]]]
      distSpec = GV.asSpec [encD, markD, (GV.transform . prefToShare) [], selectionD [],  distDat]
      encT = GV.text [GV.TSelectionCondition (GV.SelectionName "LabelsS") [] [GV.TName "District", GV.TmType GV.Nominal]] 
      lSpec = GV.asSpec [(GV.encoding . encDX . encDY . encT) [], (GV.transform . makeLabel)  [], GV.mark GV.Text [], selectionL [] , distDat]
  in FV.configuredVegaLite vc [FV.title title, GV.layer [distSpec, lSpec]]
      

somBoxes ::  Foldable f
         => T.Text
         -> FV.ViewConfig
         -> f (F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, SOM_Cluster])
         -> GV.VegaLite
somBoxes title vc rows =
  let somDat = FV.recordsToVLData id FV.defaultParse rows
      makeShare = GV.calculateAs "datum.DemPref - 0.5" "Dem Vote Share"
      makeLabel = GV.calculateAs "datum.state_abbreviation + \"-\" + datum.congressional_district" "District"
      boxPlotMark = GV.mark GV.Boxplot [GV.MExtent $ GV.IqrScale 1.0, GV.MNoOutliers]
      encX = GV.position GV.X [GV.PName "Dem Vote Share", GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [FV.pName @SOM_Cluster, GV.PmType GV.Ordinal]
      encColor = GV.color [GV.MName "Dem Vote Share", GV.MmType GV.Quantitative,  GV.MScale [GV.SScheme "redblue" []]]
      encTooltip = GV.tooltip [GV.TName "District", GV.TmType GV.Nominal]
      boxSpec = GV.asSpec [(GV.encoding . encX . encY) []
                          , boxPlotMark
                          , (GV.transform . makeShare . makeLabel) []
                          ]
      pointsSpec = GV.asSpec [(GV.encoding . encX . encY . encColor . encTooltip) []
                             , GV.mark GV.Circle []
                             ,(GV.transform . makeShare . makeLabel) []
                             ]
  in FV.configuredVegaLite vc [FV.title title
                              , GV.layer [boxSpec, pointsSpec]
                              , somDat]


           
clusterVL :: forall x y f.
             (Foldable f
             , FV.ToVLDataValue (F.ElField x)
             , FV.ToVLDataValue (F.ElField y)
             , F.ColumnHeaders '[x]
             , F.ColumnHeaders '[y]
             )
          => T.Text
          -> FV.ViewConfig
          -> f (F.Record [BR.Year, FK.ClusterId, x, y, W])
          -> f (F.Record ([BR.Year, FK.ClusterId, FK.MarkLabel, x, y, W, ET.DemPref]))
          -> GV.VegaLite
clusterVL title vc centroidRows districtRows =
  let datCentroids = FV.recordsToVLData id FV.defaultParse centroidRows
      datDistricts = FV.recordsToVLData id FV.defaultParse districtRows
      makeShare = GV.calculateAs "datum.DemPref - 0.5" "Dem Vote Share"
      makeCentroidColor = GV.calculateAs "0" "CentroidColor"
      centroidMark = GV.mark GV.Point [GV.MColor "grey", GV.MTooltip GV.TTEncoding]
      districtMark = GV.mark GV.Circle [ GV.MTooltip GV.TTData]
      encShape = GV.shape [FV.mName @FK.ClusterId, GV.MmType GV.Nominal]
      encX = GV.position GV.X [FV.pName @x, GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [FV.pName @y, GV.PmType GV.Quantitative]
      encColorD = GV.color [GV.MName "Dem Vote Share", GV.MmType GV.Quantitative, GV.MScale [GV.SScheme "redblue" []]]
      encFillD = GV.fill [GV.MName "Dem Vote Share", GV.MmType GV.Quantitative, GV.MScale [GV.SScheme "redblue" []]]
      encSizeC = GV.size [FV.mName @W, GV.MmType GV.Quantitative]
      selectionD = GV.selection
                   . GV.select "scalesD" GV.Interval [GV.BindScales, GV.Clear "click[event.shiftKey]"]
      centroidSpec = GV.asSpec [(GV.encoding . encX . encY . encSizeC . encShape) [], centroidMark, (GV.transform . makeCentroidColor) [], datCentroids]
      districtSpec = GV.asSpec [(GV.encoding . encX . encY . encColorD) [], districtMark, (GV.transform . makeShare) [], selectionD [], datDistricts]
  in FV.configuredVegaLite  vc [FV.title title, GV.layer [districtSpec]]
  

kMeansBoxes :: forall x y f.
             (Foldable f
             , FV.ToVLDataValue (F.ElField x)
             , FV.ToVLDataValue (F.ElField y)
             , F.ColumnHeaders '[x]
             , F.ColumnHeaders '[y]
             )
          => T.Text
          -> FV.ViewConfig
          -> f (F.Record [BR.Year, FK.ClusterId, x, y, W])
          -> f (F.Record ([BR.Year, FK.ClusterId, FK.MarkLabel, x, y, W, ET.DemPref]))
          -> GV.VegaLite
kMeansBoxes title vc centroidRows districtRows =
  let datCentroids = FV.recordsToVLData id FV.defaultParse centroidRows
      datDistricts = FV.recordsToVLData id FV.defaultParse districtRows
      makeShare = GV.calculateAs "datum.DemPref - 0.5" "Dem Vote Share"
      boxPlotMark = GV.mark GV.Boxplot [GV.MExtent $ GV.IqrScale 1.0]
      encX = GV.position GV.X [GV.PName "Dem Vote Share", GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [FV.pName @FK.ClusterId, GV.PmType GV.Ordinal]
      encColor = GV.color [GV.MName "Dem Vote Share", GV.MmType GV.Quantitative]
      encTooltip = GV.tooltip [GV.TName "mark_label", GV.TmType GV.Nominal]
  in FV.configuredVegaLite vc [FV.title title, (GV.encoding . encX . encY . encTooltip) [], boxPlotMark, (GV.transform . makeShare) [], datDistricts]

type DWSRow = [BR.StateAbbreviation
              , BR.CongressionalDistrict
              , ET.DemPref
              , PUMS.Citizens
              , PctWWC
              , PctNonWhite
              , PctYoung
              , SOM_Cluster
              , Pref2016]

dwsCollonnade :: BR.CellStyle (F.Record DWSRow) T.Text -> K.Colonnade K.Headed (F.Record DWSRow) K.Cell
dwsCollonnade cas =
  let x = 2
  in K.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . F.rgetField @BR.StateAbbreviation))
     <> K.headed "District" (BR.toCell cas "District" "District" (BR.numberToStyledHtml "%d" . F.rgetField @BR.CongressionalDistrict))
     <> K.headed "Cluster" (BR.toCell cas "% Under 45" "% Under 45" (BR.textToStyledHtml  . T.pack . show . F.rgetField @SOM_Cluster))
     <> K.headed "% WWC" (BR.toCell cas "% WWC" "% WWC" (BR.numberToStyledHtml "%.1f" . (*100) . F.rgetField @PctWWC))
     <> K.headed "% Non-White" (BR.toCell cas "% Non-White" "% Non-White" (BR.numberToStyledHtml "%.1f" . (*100) . F.rgetField @PctNonWhite))
     <> K.headed "% Under 45" (BR.toCell cas "% Under 45" "% Under 45" (BR.numberToStyledHtml "%.1f" . (*100) . F.rgetField @PctNonWhite))
     <> K.headed "2018 D Vote Share" (BR.toCell cas "2018 D" "2018 D" (BR.numberToStyledHtml "%.1f" . (*100) . F.rgetField @ET.DemPref))
     <> K.headed "2016 D Vote Share" (BR.toCell cas "2016 D" "2016 D" (BR.numberToStyledHtml "%.1f" . (*100) . F.rgetField @Pref2016))
       
type SDRow = [BR.StateAbbreviation
             , BR.CongressionalDistrict
             , ET.DemPref]
             

type SDMap = M.Map T.Text (F.Record [Mean, Sigma, ScaledDelta, FlipIndex])

sdCollonnade ::  BR.CellStyle (F.Record SDRow, SDMap) T.Text -> K.Colonnade K.Headed (F.Record SDRow, SDMap) K.Cell
sdCollonnade cas =
   K.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . F.rgetField @BR.StateAbbreviation . fst))
   <> K.headed "District" (BR.toCell cas "District" "District" (BR.numberToStyledHtml "%d" . F.rgetField @BR.CongressionalDistrict . fst))
   <> K.headed "2018 D Vote Share" (BR.toCell cas "2018 D" "2018 D" (BR.numberToStyledHtml "%.1f" . (*100) . F.rgetField @ET.DemPref . fst))
   <> K.headed "Mean Vote Share (Raw)" (BR.toCell cas "Mean" "Mean" (BR.maybeNumberToStyledHtml "%.1f" . fmap ((*100) . F.rgetField @Mean) . M.lookup "Raw" . snd))
   <> K.headed "Std Deviation (Raw)" (BR.toCell cas "Std Dev" "Std Deb" (BR.maybeNumberToStyledHtml "%.1f" . fmap ((*100) . F.rgetField @Sigma) . M.lookup "Raw" . snd))
   <> K.headed "Raw Scaled Delta" (BR.toCell cas "Raw" "Raw" (BR.maybeNumberToStyledHtml "%.2f" . fmap (F.rgetField @ScaledDelta) . M.lookup "Raw" . snd))
   <> K.headed "Raw Flip Index" (BR.toCell cas "Raw" "Raw" (BR.maybeNumberToStyledHtml "%.2f" . fmap (F.rgetField @FlipIndex) . M.lookup "Raw" . snd))   
   <> K.headed "tSNE Scaled Delta" (BR.toCell cas "tSNE" "tSNE" (BR.maybeNumberToStyledHtml "%.2f" . fmap (F.rgetField @ScaledDelta) . M.lookup "tSNE" . snd))
   <> K.headed "SOM Scaled Delta" (BR.toCell cas "SOM" "SOM" (BR.maybeNumberToStyledHtml "%.2f" . fmap (F.rgetField @ScaledDelta) . M.lookup "SOM" . snd))
   <> K.headed "PCA Scaled Delta" (BR.toCell cas "PCA" "PCA" (BR.maybeNumberToStyledHtml "%.2f" . fmap (F.rgetField @ScaledDelta) . M.lookup "PCA" . snd))

   

{-
-- SGM      
  let dDiff = districtDiff @DT.CatColsASER5 @PUMS.Citizens meanAbsMetric
      dMakeSimilar = districtMakeSimilar @DT.CatColsASER5 @PUMS.Citizens @[BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref] (fromIntegral . round)
      sgm0 :: SGM.SGM Int Double Word (DistrictP [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref] (DT.CatColsASER5 V.++ '[PUMS.Citizens]))
      sgm0 = SGM.makeSGM (SGM.exponential 0.1 0.5) 20 0.1 True dDiff dMakeSimilar 
      sgm = SGM.trainBatch sgm0 districtsForClustering
  K.logLE K.Info $ "SGM model map:" <> (T.pack $ show $ fmap districtId $ SGM.modelMap sgm)
-}

--clusterVL :: T.Text -> [(Double, Double, Double)

{-
vlRallyWWC :: Foldable f
                 => T.Text
                 -> FV.ViewConfig
                 -> f (F.Record [BR.StateAbbreviation, PollMargin, ExcessWWCPer, ExcessBasePer])
                 -> GV.VegaLite
vlRallyWWC title vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows
      makeWWC = GV.calculateAs "datum.PollMargin * datum.ExcessWWCPer" "Excess WWC %"
      makeBase = GV.calculateAs "datum.PollMargin * datum.ExcessBasePer" "Excess Base %"
      renameMargin = GV.calculateAs "datum.PollMargin" "Poll Margin %"
      renameSA = GV.calculateAs "datum.state_abbreviation" "State"      
      doFold = GV.foldAs ["Excess WWC %", "Excess Base %"] "Type" "Pct"
      encY = GV.position GV.Y [GV.PName "Type", GV.PmType GV.Nominal, GV.PAxis [GV.AxNoTitle]
                              , GV.PSort [GV.CustomSort $ GV.Strings ["Excess WWC %", "Excess Base %"]]
                              ]
      encX = GV.position GV.X [GV.PName "Pct"
                              , GV.PmType GV.Quantitative
                              ]
      encFacet = GV.row [GV.FName "State", GV.FmType GV.Nominal]
      encColor = GV.color [GV.MName "Type"
                          , GV.MmType GV.Nominal
                          , GV.MSort [GV.CustomSort $ GV.Strings ["Excess WWC %", "Excess Base %"]]
                          ]
      enc = GV.encoding . encX . encY . encColor . encFacet
      transform = GV.transform .  makeWWC . makeBase . renameSA . renameMargin . doFold
  in FV.configuredVegaLite vc [FV.title title, enc [], transform [], GV.mark GV.Bar [], dat]
-}
