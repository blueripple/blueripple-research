{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications  #-}
{-# LANGUAGE TypeFamilies      #-}
{-# LANGUAGE TypeOperators     #-}
module BlueRipple.Data.DemographicTypes where

import qualified BlueRipple.Data.DataFrames    as BR
import qualified BlueRipple.Data.Keyed         as K

import           Control.Arrow                  ( (>>>) )
import qualified Control.Foldl                 as FL
import qualified Control.MapReduce             as MR
import qualified Data.Array                    as A
import qualified Data.Map                      as M
import qualified Data.Monoid                   as Mon
import qualified Data.Text                     as T
import qualified Data.Serialize                as S
import qualified Data.Set                      as Set
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.Transform              as FT
import qualified Data.Vector                   as Vec
import           Data.Vinyl.TypeLevel           (type (++))
import           Data.Vinyl.Lens                (type (⊆))
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.XRec               as V
import           GHC.Generics                   ( Generic )
import           Data.Discrimination            ( Grouping )
import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Graphics.Vega.VegaLite        as GV

-- Serialize for caching
-- FI.VectorFor for frames
-- Grouping for leftJoin
-- FiniteSet for composition of aggregations

data DemographicGrouping = ASE | ASR | ASER deriving (Enum, Bounded, Eq, Ord, A.Ix, Show, Generic)
instance S.Serialize DemographicGrouping
type instance FI.VectorFor DemographicGrouping = Vec.Vector
instance Grouping DemographicGrouping
instance K.FiniteSet DemographicGrouping

type DemographicGroupingC = "DemographicGrouping" F.:-> DemographicGrouping

instance FV.ToVLDataValue (F.ElField DemographicGroupingC) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

data PopCountOfT = PC_All | PC_Citizen | PC_VAP deriving (Enum, Bounded, Eq, Ord, A.Ix, Show, Generic)
instance S.Serialize PopCountOfT
instance Grouping PopCountOfT
instance K.FiniteSet PopCountOfT
type instance FI.VectorFor PopCountOfT = Vec.Vector

type PopCountOf = "PopCountOf" F.:-> PopCountOfT
instance FV.ToVLDataValue (F.ElField PopCountOf) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

type PopCount = "PopCount" F.:-> Int

type CatColsASER = '[SimpleAgeC, SexC, CollegeGradC, SimpleRaceC]
catKeyASER :: SimpleAge -> Sex -> CollegeGrad -> SimpleRace -> F.Record CatColsASER
catKeyASER a s e r = a F.&: s F.&: e F.&: r F.&: V.RNil

allCatKeysASER = [catKeyASER a s e r | a <- [EqualOrOver, Under], e <- [NonGrad, Grad], s <- [Female, Male], r <- [NonWhite, White]]

type CatColsASE = '[SimpleAgeC, SexC, CollegeGradC]
catKeyASE :: SimpleAge -> Sex -> CollegeGrad -> F.Record CatColsASE
catKeyASE a s e = a F.&: s F.&: e F.&: V.RNil

allCatKeysASE = [catKeyASE a s e | a <- [EqualOrOver, Under], s <- [Female, Male], e <- [NonGrad, Grad]]

type CatColsASR = '[SimpleAgeC, SexC, SimpleRaceC]
catKeyASR :: SimpleAge -> Sex -> SimpleRace -> F.Record CatColsASR
catKeyASR a s r = a F.&: s F.&: r F.&: V.RNil

allCatKeysASR = [catKeyASR a s r | a <- [EqualOrOver, Under], s <- [Female, Male], r <- [NonWhite, White]]

data Sex = Female | Male deriving (Enum, Bounded, Eq, Ord, A.Ix, Show, Generic)
instance S.Serialize Sex
type instance FI.VectorFor Sex = Vec.Vector
instance Grouping Sex
instance K.FiniteSet Sex

type SexC = "Sex" F.:-> Sex

instance FV.ToVLDataValue (F.ElField SexC) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

--
data SimpleRace = NonWhite | White deriving (Eq, Ord, Enum, Bounded, A.Ix, Show, Generic)

instance S.Serialize SimpleRace

type instance FI.VectorFor SimpleRace = Vec.Vector

instance Grouping SimpleRace
instance K.FiniteSet SimpleRace

type SimpleRaceC = "SimpleRace" F.:-> SimpleRace

instance FV.ToVLDataValue (F.ElField SimpleRaceC) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

type IsCitizen = "IsCitizen" F.:-> Bool

data CollegeGrad = NonGrad | Grad deriving (Eq, Ord, Enum, Bounded, A.Ix, Show, Generic)


instance S.Serialize CollegeGrad

type instance FI.VectorFor CollegeGrad = Vec.Vector

instance Grouping CollegeGrad
instance K.FiniteSet CollegeGrad

type CollegeGradC = "CollegeGrad" F.:-> CollegeGrad

instance FV.ToVLDataValue (F.ElField CollegeGradC) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

type InCollege = "InCollege" F.:-> Bool

data SimpleAge = Under | EqualOrOver deriving (Eq, Ord, Enum, Bounded, A.Ix, Show, Generic)

instance S.Serialize SimpleAge

type instance FI.VectorFor SimpleAge = Vec.Vector

instance Grouping SimpleAge
instance K.FiniteSet SimpleAge

type SimpleAgeC = "SimpleAge" F.:-> SimpleAge
instance FV.ToVLDataValue (F.ElField SimpleAgeC) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)


data Age4 = A4_18To24 | A4_25To44 | A4_45To64 | A4_65AndOver deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize Age4
type instance FI.VectorFor Age4 = Vec.Vector
instance Grouping Age4
instance K.FiniteSet Age4

type Age4C = "Age4" F.:-> Age4

simpleAgeFrom4 :: SimpleAge -> [Age4]
simpleAgeFrom4 Under       = [A4_18To24, A4_25To44]
simpleAgeFrom4 EqualOrOver = [A4_45To64, A4_65AndOver]

age4ToSimple :: Age4 -> SimpleAge
age4ToSimple A4_18To24 = Under
age4ToSimple A4_25To44 = Under
age4ToSimple _ = EqualOrOver


data Age5 = A5_18To24 | A5_25To44 | A5_45To64 | A5_65To74 | A5_75AndOver deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize Age5
type instance FI.VectorFor Age5 = Vec.Vector
instance Grouping Age5
instance K.FiniteSet Age5

type Age5C = "Age5" F.:-> Age5

simpleAgeFrom5 :: SimpleAge -> [Age5]
simpleAgeFrom5 Under       = [A5_18To24, A5_25To44]
simpleAgeFrom5 EqualOrOver = [A5_45To64, A5_65To74, A5_75AndOver]

data Education = L9 | L12 | HS | SC | AS | BA | AD deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize Education
type instance FI.VectorFor Education = Vec.Vector
instance Grouping Education
instance K.FiniteSet Education

type EducationC = "Education" F.:-> Education

acsLevels :: CollegeGrad -> [Education]
acsLevels NonGrad = [L9, L12, HS, SC, AS]
acsLevels Grad    = [BA, AD]

turnoutLevels :: CollegeGrad -> [Education]
turnoutLevels NonGrad = [L9, L12, HS, SC] -- NB: Turnout data did not contain an Associates Degree row
turnoutLevels Grad    = [BA, AD]

aseACSLabel :: (Age4, Sex, Education) -> T.Text
aseACSLabel (a, s, e) = sexLabel s <> age4Label a <> acsEducationLabel e

age4Label :: Age4 -> T.Text
age4Label A4_18To24    = "18To24"
age4Label A4_25To44    = "25To44"
age4Label A4_45To64    = "45To64"
age4Label A4_65AndOver = "65AndOver"

sexLabel :: Sex -> T.Text
sexLabel Female = "Female"
sexLabel Male   = "Male"

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


data ACSRace = ACS_All | ACS_WhiteNonHispanic | ACS_NonWhite deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize ACSRace
type instance FI.VectorFor ACSRace = Vec.Vector
instance Grouping ACSRace
instance K.FiniteSet ACSRace

type ACSRaceC = "ACSRace" F.:-> ACSRace


acsRaceLabel :: ACSRace -> T.Text
acsRaceLabel ACS_All              = "All"
acsRaceLabel ACS_WhiteNonHispanic = "WhiteNonHispanic"
acsRaceLabel ACS_NonWhite         = "NonWhite"

asrACSLabel :: (Age5, Sex, ACSRace) -> T.Text
asrACSLabel (a, s, r) = sexLabel s <> acsRaceLabel r <> age5Label a

acsRaceLabel' :: ACSRace -> T.Text
acsRaceLabel' ACS_All              = ""
acsRaceLabel' ACS_WhiteNonHispanic = "WhiteNonHispanic"
acsRaceLabel' ACS_NonWhite         = "NonWhite"

asrACSLabel' :: (Age5, Sex, ACSRace) -> T.Text
asrACSLabel' (a, s, r) = sexLabel s <> acsRaceLabel' r <> age5Label a


asACSLabel :: (Age5, Sex) -> T.Text
asACSLabel (a, s) = sexLabel s <> age5Label a


data TurnoutRace = Turnout_All | Turnout_WhiteNonHispanic | Turnout_Black | Turnout_Asian | Turnout_Hispanic deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize TurnoutRace
type instance FI.VectorFor TurnoutRace = Vec.Vector
instance Grouping TurnoutRace
instance K.FiniteSet TurnoutRace

type TurnoutRaceC = "TurnoutRace" F.:-> TurnoutRace

data Race5 = R5_Other | R5_Black | R5_Latinx | R5_Asian | R5_WhiteNonLatinx deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize Race5
type instance FI.VectorFor Race5 = Vec.Vector
instance Grouping Race5
instance K.FiniteSet Race5

type Race5C = "Race5" F.:-> Race5
instance FV.ToVLDataValue (F.ElField Race5C) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

simpleRaceFromRace5 :: Race5 -> SimpleRace
simpleRaceFromRace5 R5_Other = NonWhite
simpleRaceFromRace5 R5_Black = NonWhite
simpleRaceFromRace5 R5_Latinx = NonWhite
simpleRaceFromRace5 R5_Asian = NonWhite
simpleRaceFromRace5 R5_WhiteNonLatinx = White

turnoutRaceLabel :: TurnoutRace -> T.Text
turnoutRaceLabel Turnout_All              = "All"
turnoutRaceLabel Turnout_WhiteNonHispanic = "WhiteNonHispanic"
turnoutRaceLabel Turnout_Black            = "Black"
turnoutRaceLabel Turnout_Asian            = "Asian"
turnoutRaceLabel Turnout_Hispanic         = "Hispanic"


asrTurnoutLabel' :: (Age5, Sex, TurnoutRace) -> T.Text
asrTurnoutLabel' (a, s, r) = turnoutRaceLabel r <> sexLabel s <> age5Label a


asrTurnoutLabel :: (Age5, Sex, ACSRace) -> T.Text
asrTurnoutLabel (a, s, r) = acsRaceLabel r <> sexLabel s <> age5Label a


acsASELabelMap :: M.Map T.Text (Age4, Sex, Education)
acsASELabelMap =
  M.fromList
    . fmap (\x -> (aseACSLabel x, x))
    $ [ (a, s, e)
      | a <- [(minBound :: Age4) ..]
      , s <- [(minBound :: Sex) ..]
      , e <- [(minBound :: Education) ..]
      ]

allTextKeys
  :: forall t
   . (V.KnownField t, V.Snd t ~ T.Text)
  => [T.Text]
  -> Set.Set (F.Record '[t])
allTextKeys = Set.fromList . fmap (\x -> x F.&: V.RNil)


allASE_ACSKeys = allTextKeys @BR.ACSKey $ fmap
  aseACSLabel
  [ (a, s, e)
  | a <- [(minBound :: Age4) ..]
  , s <- [(minBound :: Sex) ..]
  , e <- [(minBound :: Education) ..]
  ]

allASR_ACSKeys = allTextKeys @BR.ACSKey $ fmap
  asrACSLabel'
  [ (a, s, r)
  | a <- [(minBound :: Age5) ..]
  , s <- [(minBound :: Sex) ..]
  , r <- [ACS_All, ACS_WhiteNonHispanic]
  ] -- this is how we have the data.  We infer ACS_White from .this

allASR_TurnoutKeys = allTextKeys @BR.Group $ fmap
  asrTurnoutLabel'
  [ (a, s, r)
  | a <- [(minBound :: Age5) ..]
  , s <- [(minBound :: Sex) ..]
  , r <- [(minBound :: TurnoutRace) ..]
  ]

allASE_TurnoutKeys = allTextKeys @BR.Group $ fmap
  aseTurnoutLabel
  [ (a, s, e)
  | a <- [(minBound :: Age5) ..]
  , s <- [(minBound :: Sex) ..]
  , e <- [L9, L12, HS, SC, BA, AD]
  ] -- no associates degree in turnout data

typedASEDemographics
  :: (F.ElemOf rs BR.ACSKey)
  => F.Record rs
  -> Either T.Text (F.Record (rs ++ '[Age4C, SexC, EducationC]))
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

-- aggregate ACSKey to as
aggACS_ASEKey :: K.AggListRec Int '[Age4C, SexC, EducationC] '[BR.ACSKey]
aggACS_ASEKey =
  K.AggList
    $ K.aggList
    . (\x -> [x F.&: V.RNil])
    . aseACSLabel
    . (\r ->
        (F.rgetField @Age4C r, F.rgetField @SexC r, F.rgetField @EducationC r)
      )

aggACS_ASRKey :: K.AggListRec Int '[Age5C, SexC, ACSRaceC] '[BR.ACSKey]
aggACS_ASRKey =
  K.AggList
    $ K.aggList
    . (\x -> [x F.&: V.RNil])
    . asrACSLabel'
    . (\r ->
        (F.rgetField @Age5C r, F.rgetField @SexC r, F.rgetField @ACSRaceC r)
      )

type ACSKeys = [BR.Year, BR.StateFIPS, BR.CongressionalDistrict, BR.StateName, BR.StateAbbreviation]

acsCountMonoidOps :: K.MonoidOps (F.Record '[BR.ACSCount])
acsCountMonoidOps = K.monoidOpsFromFold $ FF.foldAllConstrained @Num FL.sum

acsCountGroupOps :: K.GroupOps (F.Record '[BR.ACSCount])
acsCountGroupOps = K.GroupOps
  acsCountMonoidOps
  (FT.recordSingleton . negate . F.rgetField @BR.ACSCount)

-- We can't check these for exactness because T.Text is not finite.
-- Maybe we should add checks for a finite lits of values and implement
-- exact and complete in terms of that?
simplifyACS_ASEFold
  :: FL.FoldM
       (Either T.Text)
       BR.ASEDemographics
       ( F.FrameRec
           (ACSKeys ++ '[SimpleAgeC, SexC, CollegeGradC, BR.ACSCount])
       )
simplifyACS_ASEFold =
  let
    aggAge4         = K.toAggListRec $ K.liftAggList simpleAgeFrom4
    aggACSEducation = K.toAggListRec $ K.liftAggList acsLevels
    aggSex          = K.toAggListRec $ K.liftAggList (pure . id)
    aggASE =
      aggAge4 `K.aggListProductRec` aggSex `K.aggListProductRec` aggACSEducation
    agg = aggASE >>> aggACS_ASEKey -- when all is said and done, aggregations compose nicely
  in
    FMR.concatFoldM $ FMR.mapReduceFoldM
      (FMR.generalizeUnpack MR.noUnpack)
      ( FMR.generalizeAssign
      $ FMR.assignKeysAndData @ACSKeys @'[BR.ACSKey, BR.ACSCount]
      )
      (FMR.makeRecsWithKeyM id $ MR.ReduceFoldM $ const $ K.aggFoldAllCheckedRec
        (K.hasOneOfEachRec allASE_ACSKeys)
        (K.functionalize agg)
        (K.groupCollapse acsCountGroupOps)
      )

simplifyACS_ASRFold
  :: FL.FoldM
       (Either T.Text)
       BR.ASEDemographics
       (F.FrameRec (ACSKeys ++ '[SimpleAgeC, SexC, SimpleRaceC, BR.ACSCount]))
simplifyACS_ASRFold
  = let
      aggAge5    = K.toAggListRec $ K.liftAggList simpleAgeFrom5
      aggACSRace = K.toAggListRec $ K.AggList $ \sr -> case sr of
        NonWhite -> K.IndexedList [(1, ACS_All), (-1, ACS_WhiteNonHispanic)]
        White    -> K.aggList [ACS_WhiteNonHispanic]
      aggSex = K.toAggListRec $ K.liftAggList pure
      aggASR =
        aggAge5 `K.aggListProductRec` aggSex `K.aggListProductRec` aggACSRace
      agg = aggASR >>> aggACS_ASRKey -- when all is said and done, aggregations compose nicely
    in
      FMR.concatFoldM $ FMR.mapReduceFoldM
        (FMR.generalizeUnpack MR.noUnpack)
        ( FMR.generalizeAssign
        $ FMR.assignKeysAndData @ACSKeys @'[BR.ACSKey, BR.ACSCount]
        )
        ( FMR.makeRecsWithKeyM id
        $ MR.ReduceFoldM
        $ const
        $ K.aggFoldAllCheckedRec (K.hasOneOfEachRec allASR_ACSKeys)
                                 (K.functionalize agg)
                                 (K.groupCollapse acsCountGroupOps)
        )

turnoutMonoidOps
  :: K.MonoidOps
       (F.Record '[BR.Population, BR.Citizen, BR.Registered, BR.Voted])
turnoutMonoidOps = K.monoidOpsFromFold $ FF.foldAllConstrained @Num FL.sum

turnoutGroupOps
  :: K.GroupOps (F.Record '[BR.Population, BR.Citizen, BR.Registered, BR.Voted])
turnoutGroupOps =
  let pop  = F.rgetField @BR.Population 
      cit  = F.rgetField @BR.Population 
      reg  = F.rgetField @BR.Registered 
      voted  = F.rgetField @BR.Voted 
      invert r =
        negate (pop r)
          F.&: negate (cit r)
          F.&: negate (reg r)
          F.&: negate (voted r)
          F.&: V.RNil
  in  K.GroupOps turnoutMonoidOps invert

aggTurnout_ASEKey :: K.AggListRec Int '[Age5C, SexC, EducationC] '[BR.Group]
aggTurnout_ASEKey =
  K.AggList
    $ K.aggList
    . (\x -> [x F.&: V.RNil])
    . aseTurnoutLabel
    . (\r ->
        (F.rgetField @Age5C r, F.rgetField @SexC r, F.rgetField @EducationC r)
      )

turnoutSimpleASEAgg
  :: K.AggListRec Int '[SimpleAgeC, SexC, CollegeGradC] '[BR.Group]
turnoutSimpleASEAgg =
  let
    aggAge5      = K.toAggListRec $ K.liftAggList simpleAgeFrom5
    aggSex       = K.toAggListRec $ K.liftAggList (pure . id)
    aggEducation = K.toAggListRec $ K.liftAggList turnoutLevels
    aggASE =
      aggAge5 `K.aggListProductRec` aggSex `K.aggListProductRec` aggEducation
  in
    aggASE >>> aggTurnout_ASEKey

simplifyTurnoutASEFold
  :: FL.FoldM
       (Either T.Text)
       BR.TurnoutASE
       ( F.FrameRec
           '[BR.Year, SimpleAgeC, SexC, CollegeGradC, BR.Population, BR.Citizen, BR.Registered, BR.Voted]
       )
simplifyTurnoutASEFold = FMR.concatFoldM $ FMR.mapReduceFoldM
  (MR.generalizeUnpack MR.noUnpack)
  ( MR.generalizeAssign
  $ FMR.assignKeysAndData @'[BR.Year]
    @'[BR.Group, BR.Population, BR.Citizen, BR.Registered, BR.Voted]
  )
  (FMR.makeRecsWithKeyM id $ MR.ReduceFoldM $ const $ K.aggFoldAllCheckedRec
    (K.hasOneOfEachRec allASE_TurnoutKeys)
    (K.functionalize turnoutSimpleASEAgg)
    (K.groupCollapse turnoutGroupOps)
  )

aggTurnout_ASRKey :: K.AggListRec Int '[Age5C, SexC, TurnoutRaceC] '[BR.Group]
aggTurnout_ASRKey =
  K.AggList
    $ K.aggList
    . (\x -> [x F.&: V.RNil])
    . asrTurnoutLabel'
    . (\r ->
        (F.rgetField @Age5C r, F.rgetField @SexC r, F.rgetField @TurnoutRaceC r)
      )

turnoutSimpleASRAgg
  :: K.AggListRec Int '[SimpleAgeC, SexC, SimpleRaceC] '[BR.Group]
turnoutSimpleASRAgg
  = let
      aggAge5 = K.toAggListRec $ K.liftAggList simpleAgeFrom5
      aggSex  = K.toAggListRec $ K.liftAggList (pure . id)
      aggRace = K.toAggListRec $ K.AggList $ \sr -> case sr of
        NonWhite ->
          K.IndexedList [(1, Turnout_All), (-1, Turnout_WhiteNonHispanic)]
        White -> K.aggList [Turnout_WhiteNonHispanic]
      aggASR =
        aggAge5 `K.aggListProductRec` aggSex `K.aggListProductRec` aggRace
    in
      aggASR >>> aggTurnout_ASRKey

simplifyTurnoutASRFold
  :: FL.FoldM
       (Either T.Text)
       BR.TurnoutASR
       ( F.FrameRec
           '[BR.Year, SimpleAgeC, SexC, SimpleRaceC, BR.Population, BR.Citizen, BR.Registered, BR.Voted]
       )
simplifyTurnoutASRFold = FMR.concatFoldM $ FMR.mapReduceFoldM
  (MR.generalizeUnpack MR.noUnpack)
  ( MR.generalizeAssign
  $ FMR.assignKeysAndData @'[BR.Year]
    @'[BR.Group, BR.Population, BR.Citizen, BR.Registered, BR.Voted]
  )
  (FMR.makeRecsWithKeyM id $ MR.ReduceFoldM $ const $ K.aggFoldAllCheckedRec
    (K.hasOneOfEachRec allASR_TurnoutKeys)
    (K.functionalize turnoutSimpleASRAgg)
    (K.groupCollapse turnoutGroupOps)
  )

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
  -> Either T.Text (F.Record (rs ++ '[Age5C, SexC, EducationC]))
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


