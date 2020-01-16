{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveFunctor     #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications  #-}
{-# LANGUAGE TypeFamilies      #-}
{-# LANGUAGE TypeOperators     #-}
{-# LANGUAGE TupleSections     #-}
module BlueRipple.Data.DemographicTypes where

import qualified BlueRipple.Data.DataFrames    as BR

import qualified Control.Foldl                 as FL
import qualified Data.Array                    as A
import qualified Data.Map                      as M
import qualified Data.Text                     as T
import qualified Data.Serialize                as S
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Data.Vector                   as Vec
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import           GHC.Generics                   ( Generic )
import           Data.Discrimination            ( Grouping )
import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Graphics.Vega.VegaLite        as GV

-- Serialize for caching
-- FI.VectorFor for frames
-- Grouping for leftJoin

data Sex = Female | Male deriving (Enum, Bounded, Eq, Ord, A.Ix, Show, Generic)

instance S.Serialize Sex

type instance FI.VectorFor Sex = Vec.Vector

instance Grouping Sex

type SexC = "Sex" F.:-> Sex

instance FV.ToVLDataValue (F.ElField SexC) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

--
data SimpleRace = NonWhite | White deriving (Eq, Ord, Enum, Bounded, A.Ix, Show, Generic)

instance S.Serialize SimpleRace

type instance FI.VectorFor SimpleRace = Vec.Vector

instance Grouping SimpleRace

type SimpleRaceC = "SimpleRace" F.:-> SimpleRace

instance FV.ToVLDataValue (F.ElField SimpleRaceC) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

data CollegeGrad = NonGrad | Grad deriving (Eq, Ord, Enum, Bounded, A.Ix, Show, Generic)

instance S.Serialize CollegeGrad

type instance FI.VectorFor CollegeGrad = Vec.Vector

instance Grouping CollegeGrad

type CollegeGradC = "CollegeGrad" F.:-> CollegeGrad

instance FV.ToVLDataValue (F.ElField CollegeGradC) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

data SimpleAge = Under | EqualOrOver deriving (Eq, Ord, Enum, Bounded, A.Ix, Show, Generic)

instance S.Serialize SimpleAge

type instance FI.VectorFor SimpleAge = Vec.Vector

instance Grouping SimpleAge

type SimpleAgeC = "SimpleAge" F.:-> SimpleAge
instance FV.ToVLDataValue (F.ElField SimpleAgeC) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)


data Age4 = A4_18To24 | A4_25To44 | A4_45To64 | A4_65AndOver deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize Age4
type instance FI.VectorFor Age4 = Vec.Vector
instance Grouping Age4

type Age4C = "Age4" F.:-> Age4

simpleAgeFrom4 :: SimpleAge -> [Age4]
simpleAgeFrom4 Under       = [A4_18To24, A4_25To44]
simpleAgeFrom4 EqualOrOver = [A4_45To64, A4_65AndOver]

data Age5 = A5_18To24 | A5_25To44 | A5_45To64 | A5_65To74 | A5_75AndOver deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize Age5
type instance FI.VectorFor Age5 = Vec.Vector
instance Grouping Age5

type Age5C = "Age5" F.:-> Age5

simpleAgeFrom5 :: SimpleAge -> [Age5]
simpleAgeFrom5 Under       = [A5_18To24, A5_25To44]
simpleAgeFrom5 EqualOrOver = [A5_45To64, A5_65To74, A5_75AndOver]

data Education = L9 | L12 | HS | SC | AS | BA | AD deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize Education
type instance FI.VectorFor Education = Vec.Vector
instance Grouping Education

type EducationC = "Education" F.:-> Education

acsLevels :: CollegeGrad -> [Education]
acsLevels NonGrad = [L9, L12, HS, SC, AS]
acsLevels Grad    = [BA, AD]

turnoutLevels :: CollegeGrad -> [Education]
turnoutLevels NonGrad = [L9, L12, HS, SC] -- NB: Turnout data did not contain an Associates Degree row
turnoutLevels Grad    = [BA, AD]

aseACSLabel :: (Age4, Sex, Education) -> T.Text
aseACSLabel (a, s, e) = acsSexLabel s <> age4Label a <> acsEducationLabel e

age4Label :: Age4 -> T.Text
age4Label A4_18To24    = "18To24"
age4Label A4_25To44    = "25To44"
age4Label A4_45To64    = "45To64"
age4Label A4_65AndOver = "65AndOver"

acsSexLabel :: Sex -> T.Text
acsSexLabel Female = "Female"
acsSexLabel Male   = "Male"

acsEducationLabel :: Education -> T.Text
acsEducationLabel L9  = "LessThan9th"
acsEducationLabel L12 = "LessThan12th"
acsEducationLabel HS  = "HighSchool"
acsEducationLabel SC  = "SomeCollege"
acsEducationLabel AS  = "Associates"
acsEducationLabel BA  = "Bachelors"
acsEducationLabel AD  = "AdvancedDegree"

aseTurnoutLabel :: (Age5, Sex, Education) -> T.Text
aseTurnoutLabel (a, s, e) =
  turnoutSexLabel s <> age5Label a <> turnoutEducationLabel e

age5Label :: Age5 -> T.Text
age5Label A5_18To24    = "18To24"
age5Label A5_25To44    = "25To44"
age5Label A5_45To64    = "45To64"
age5Label A5_65To74    = "65To74"
age5Label A5_75AndOver = "75AndOver"

turnoutSexLabel :: Sex -> T.Text
turnoutSexLabel Female = "F"
turnoutSexLabel Male   = "M"

turnoutEducationLabel :: Education -> T.Text
turnoutEducationLabel L9  = "L9"
turnoutEducationLabel L12 = "L12"
turnoutEducationLabel HS  = "HS"
turnoutEducationLabel SC  = "SC"
turnoutEducationLabel AS =
  error "No associates degree category in turnout data."
turnoutEducationLabel BA = "BA"
turnoutEducationLabel AD = "AD"


data ACSRace = ACS_All | ACS_WhiteNonHispanic | ACS_NonWhite deriving (Enum, Bounded, Eq, Ord, Show)
data TurnoutRace = Turnout_White | Turnout_Black | Turnout_Asian | Turnout_Hispanic

acsRaceLabel :: ACSRace -> T.Text
acsRaceLabel ACS_All              = "All"
acsRaceLabel ACS_WhiteNonHispanic = "WhiteNonHispanic"
acsRaceLabel ACS_NonWhite         = "NonWhite"

asrACSLabel :: (Age5, Sex, ACSRace) -> T.Text
asrACSLabel (a, s, r) = acsSexLabel s <> acsRaceLabel r <> age5Label a

asACSLabel :: (Age5, Sex) -> T.Text
asACSLabel (a, s) = acsSexLabel s <> age5Label a

asrTurnoutLabel :: (Age5, Sex, ACSRace) -> T.Text
asrTurnoutLabel (a, s, r) = acsRaceLabel r <> acsSexLabel s <> age5Label a


acsASELabelMap :: M.Map T.Text (Age4, Sex, Education)
acsASELabelMap =
  M.fromList
    . fmap (\x -> (aseACSLabel x, x))
    $ [ (a, s, e)
      | a <- [(minBound :: Age4) ..]
      , s <- [(minBound :: Sex) ..]
      , e <- [(minBound :: Education) ..]
      ]

typedASEDemographics
  :: (F.ElemOf rs BR.ACSKey)
  => F.Record rs
  -> Either T.Text (F.Record (rs V.++ '[Age4C, SexC, EducationC]))
typedASEDemographics r = do
  let key     = F.rgetField @BR.ACSKey r
      textMap = T.pack $ show acsASELabelMap
      errMsg  = key <> " not found in " <> textMap
      toRec :: (Age4, Sex, Education) -> F.Record '[Age4C, SexC, EducationC]
      toRec (a, s, e) = a F.&: s F.&: e F.&: V.RNil
  typedCols <- fmap toRec $ maybe (Left errMsg) Right $ M.lookup
    key
    acsASELabelMap
  return $ r `V.rappend` typedCols

demographicsFold :: FL.Fold (F.Record '[BR.ACSCount]) (F.Record '[BR.ACSCount])
demographicsFold = FF.foldAllConstrained @Num FL.sum

turnoutASELabelMap :: M.Map T.Text (Age5, Sex, Education)
turnoutASELabelMap =
  M.fromList
    . fmap (\x -> (aseTurnoutLabel x, x))
    $ [ (a, s, e)
      | a <- [(minBound :: Age5) ..]
      , s <- [(minBound :: Sex) ..]
      , e <- [(minBound :: Education) ..]
      ]

typedASETurnout
  :: (F.ElemOf rs BR.Group)
  => F.Record rs
  -> Either T.Text (F.Record (rs V.++ '[Age5C, SexC, EducationC]))
typedASETurnout r = do
  let key     = F.rgetField @BR.Group r
      textMap = T.pack $ show turnoutASELabelMap
      errMsg  = key <> " not found in " <> textMap
      toRec :: (Age5, Sex, Education) -> F.Record '[Age5C, SexC, EducationC]
      toRec (a, s, e) = a F.&: s F.&: e F.&: V.RNil
  typedCols <- fmap toRec $ maybe (Left errMsg) Right $ M.lookup
    key
    turnoutASELabelMap
  return $ r `V.rappend` typedCols


{-
Lets talk about aggregation!
Suppose we have:
keys, a of type A
keys, b of type B
data, d of type D
An arrow, gA : A -> D, mapping keys in a to data, think the data in a row indexed by A
An arrow,  aggBA : B -> Z[A], where Z[A] is the module of finite formal linear combinations of a in A with coeficients in Z
An arrow, fold: Z[D] -> D, "folding" formal linear combinations of d in D (with coefficients in Z) into a d in D
There is a covariant functor, FZ : Set -> Category of modules, FZ (X) = Z[X] and FZ (g : X -> Y) = Z[X] -> Z[Y]
Then we can construct gB : B -> D, gB (b) = fold . FZ (gA) . aggBA b

Suppose we have gAX : A x X -> D
And aggBA : B -> Z[A]
and aggYX : X -> Z[Y].
Let's denote the coefficient in Z of the element A in some g in Z[A] via cA(g)
And the tensor product of a in A and x in X as (a @ x). [We're doing this all in Hask, so a @ x = (a,x)]
We can compose them to yield aggBXAY : B x X -> Z[A x Y], aggBC (b @ c) = sum_a sum_x (cA(aggBA b) + cX(aggYX x)) (a @ x)
So we have gBY (b @ y) = fold . FZ (gAX) . aggBYAX (b @ y)

Some thoughts:

We don't need gA if all we want is gBY.  We only ever need a "getter" for the aggregated key.
"fold" is the same, regardless of the aggregation since it is only a rule for collapsing the formal sum.  (Wait.  It's an F-algebra of FZ!)

To summarize:  TO do an aggregation we need:
1) a getter on the final key
2) an aggregation from the inital to the final key, which we can make from aggregations on sub-keys
3) an F-algebra (D, fold)

-}
-- finite formal sum of @a@ with integer coefficients
data AggSum a where
  AggSum :: Ord a => M.Map a Int -> AggSum a
  deriving (Show, Functor)

sum :: Ord a => [a] -> AggSum a
sum as = AggSum $ M.fromList $ fmap (, 1) as

diff :: Ord a => a -> a -> AggSum a
diff a1 a2 = AggSum $ M.fromListWith (+) [(a1, 1), (a2, -1)]

diffSum :: Ord a => a -> [a] -> AggSum a
diffSum a as = AggSum $ M.fromListWith (+) $ (a, 1) : fmap (, -1) as

type Aggregation b a = b -> AggSum a

composeAggSums :: AggSum a -> AggSum b -> AggSum (a, b)
composeAggSums (AggSum ma) (AggSum mb) = AggSum $ M.fromListWith (+) $ do
  aa <- M.toList ma
  ab <- M.toList mb
  return ((fst aa, fst ab), (snd aa + snd ab))

composeAggregations
  :: Aggregation b a -> Aggregation y x -> Aggregation (b, y) (a, x)
composeAggregations aggBA aggYX (b, y) = composeAggSums (aggBA b) (aggYX y)

aggFold
  :: forall k k' d
   . Aggregation k' k
  -> FL.Fold (d, Int) d
  -> [k']
  -> FL.FoldM (Either T.Text) (k, d) [(k', d)]
aggFold agg alg newKeys = FMR.postMapM go (FL.generalize FL.map)
 where
  go :: M.Map k d -> Either T.Text [(k', d)]
  go m = traverse (doOne m) newKeys
  doOne :: M.Map k d -> k' -> (k', d)
  doOne = traverse (`M.lookup` m) $ agg k'



  {-
data AggExpr a where
  AggSingle :: a -> AggExpr a
  AggSum :: [AggExpr a] -> AggExpr a
  AggDiff :: AggExpr a -> AggExpr a -> AggExpr a
  deriving (Functor, Show)

aggregate :: Num b => (a -> b) -> AggExpr a -> b
aggregate f (AggSingle a ) = f a
aggregate f (AggSum    as) = FL.fold (FL.premap (aggregate f) FL.sum) as
aggregate f (AggDiff a a') = aggregate f a - aggregate f a'

aggregateM :: (Monad m, Num b) => (a -> m b) -> AggExpr a -> m b
aggregateM f (AggSingle a) = f a
aggregateM f (AggSum as) =
  FL.foldM (FL.premapM (aggregateM f) (FL.generalize FL.sum)) as
aggregateM f (AggDiff a a') = (-) <$> aggregateM f a <*> aggregateM f a'

composeAggExpr :: AggExpr a -> AggExpr b -> AggExpr (a, b)
composeAggExpr (AggSingle a) (AggSingle b) = AggSingle (a, b)
composeAggExpr (AggSum as) aeb = AggSum $ fmap (`composeAggExpr` aeb) as
composeAggExpr (AggDiff a a') aeb =
  AggDiff (composeAggExpr a aeb) (composeAggExpr a' aeb)
composeAggExpr aea (AggSum bs) = AggSum $ fmap (composeAggExpr aea) bs
composeAggExpr aea (AggDiff b b') =
  AggDiff (composeAggExpr aea b) (composeAggExpr aea b')

aggAge4ToSimple :: SimpleAge -> AggExpr Age4
aggAge4ToSimple x = AggSum $ fmap AggSingle $ simpleAgeFrom4 x

aggAge5ToSimple :: SimpleAge -> AggExpr Age5
aggAge5ToSimple x = AggSum $ fmap AggSingle $ simpleAgeFrom5 x

aggACSToCollegeGrad :: CollegeGrad -> AggExpr Education
aggACSToCollegeGrad x = AggSum $ fmap AggSingle $ acsLevels x

aggTurnoutToCollegeGrad :: CollegeGrad -> AggExpr Education
aggTurnoutToCollegeGrad x = AggSum $ fmap AggSingle $ turnoutLevels x

aggSexToAll :: () -> AggExpr Sex
aggSexToAll _ = AggSum $ fmap AggSingle [Female, Male]

aggTurnoutRaceToSimple :: SimpleRace -> AggExpr TurnoutRace
aggTurnoutRaceToSimple NonWhite =
  AggSum $ fmap AggSingle [Turnout_Black, Turnout_Asian, Turnout_Hispanic]
aggTurnoutRaceToSimple White = AggSingle Turnout_White

aggACSRaceToSimple :: SimpleRace -> AggExpr ACSRace
aggACSRaceToSimple NonWhite =
  AggDiff (AggSingle ACS_All) (AggSingle ACS_WhiteNonHispanic)
aggACSRaceToSimple White = AggSingle ACS_WhiteNonHispanic

aggAE_ACS :: (SimpleAge, CollegeGrad) -> AggExpr (Age4, Education)
aggAE_ACS (sa, cg) =
  composeAggExpr (aggAge4ToSimple sa) (aggACSToCollegeGrad cg)

aggAR_ACS :: (SimpleAge, SimpleRace) -> AggExpr (Age5, ACSRace)
aggAR_ACS (sa, sr) =
  composeAggExpr (aggAge5ToSimple sa) (aggACSRaceToSimple sr)

aggAE_Turnout :: (SimpleAge, CollegeGrad) -> AggExpr (Age5, Education)
aggAE_Turnout (sa, cg) =
  composeAggExpr (aggAge5ToSimple sa) (aggTurnoutToCollegeGrad cg)

aggAR_Turnout :: (SimpleAge, SimpleRace) -> AggExpr (Age5, TurnoutRace)
aggAR_Turnout (sa, sr) =
  composeAggExpr (aggAge5ToSimple sa) (aggTurnoutRaceToSimple sr)

aggFold
  :: forall k k' v
   . (Show k, Show k', Show v, Num v, Ord k)
  => [(k', AggExpr k)]
  -> FL.FoldM (Either T.Text) (k, v) [(k', v)]
aggFold keyedAE = FMR.postMapM go (FL.generalize FL.map)
 where
  getOne :: M.Map k v -> (k', AggExpr k) -> Either T.Text (k', v)
  getOne m (k', ae) =
    fmap (k', )
      $ maybe
          (  Left
          $  "lookup failed in aggFold: aggExpr="
          <> (T.pack $ show ae)
          <> "; m="
          <> (T.pack $ show m)
          )
          Right
      $ aggregateM (`M.lookup` m) ae
  go :: M.Map k v -> Either T.Text [(k', v)]
  go m = traverse (getOne m) keyedAE

aggFoldWeighted
  :: forall k k' v w
   . (Show k, Show k', Show v, Num v, Num w, Ord k)
  => [(k', AggExpr k)]
  -> FL.FoldM (Either T.Text) (k, (w, v)) [(k', v)]
aggFoldWeighted keyedAE = FMR.postMapM go (FL.generalize FL.map)
 where
  getOne :: M.Map k v -> (k', AggExpr k) -> Either T.Text (k', v)
  getOne m (k', ae) =
    fmap (k', )
      $ maybe
          (  Left
          $  "lookup failed in aggFold: aggExpr="
          <> (T.pack $ show ae)
          <> "; m="
          <> (T.pack $ show m)
          )
          Right
      $ aggregateM (`M.lookup` m) ae
  go :: M.Map k v -> Either T.Text [(k', v)]
  go m = traverse (getOne m) keyedAE  

aggFoldRecords :: [(F.Record k', AggExp (V.Snd k'))] -> FL.FoldM (F.Record (k V.++ v) 
-}
