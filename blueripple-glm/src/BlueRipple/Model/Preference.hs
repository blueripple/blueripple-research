{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE UndecidableInstances      #-}

module BlueRipple.Model.Preference where

import qualified Control.Foldl                 as FL
import qualified Control.Lens                  as L
import qualified Control.Monad.Except          as X
import           Control.Monad.IO.Class         ( MonadIO(liftIO) )
import qualified Colonnade                     as C
import qualified Text.Blaze.Colonnade          as BC
import qualified Text.Blaze.Html               as BH
import qualified Text.Blaze.Html5.Attributes   as BHA
import qualified Data.List                     as L
import qualified Data.Map                      as M
import qualified Data.Array                    as A
import qualified Data.Vector                   as VB
import qualified Data.Vector.Storable          as VS

import qualified Text.Pandoc.Error             as PA

import qualified Data.Profunctor               as PF
import qualified Data.Text                     as T
import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Clock               as Time
import qualified Data.Time.Format              as Time
import qualified Data.Vinyl                    as V
import qualified Text.Printf                   as PF
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.CSV                    as F
import qualified Frames.InCore                 as F
                                         hiding ( inCoreAoS )

import qualified Pipes                         as P
import qualified Pipes.Prelude                 as P
import qualified Statistics.Types              as S
import qualified Statistics.Distribution       as S
import qualified Statistics.Distribution.StudentT      as S

import qualified Numeric.LinearAlgebra         as LA

import qualified Graphics.Vega.VegaLite        as GV
import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV

import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Frames.Visualization.VegaLite.StackedArea
                                               as FV
import qualified Frames.Visualization.VegaLite.LineVsTime
                                               as FV
import qualified Frames.Visualization.VegaLite.ParameterPlots
                                               as FV

import qualified Frames.Visualization.VegaLite.Correlation
                                               as FV

import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as MR
import qualified Frames.Enumerations           as FE

import qualified Knit.Report                   as K
import qualified Knit.Report.Input.MarkDown.PandocMarkDown    as K
import           Polysemy.Error                 (Error)
import qualified Text.Pandoc.Options           as PA

import           Data.String.Here               ( here, i )
import qualified Relude.Extra as Relude
import           BlueRipple.Configuration
import           BlueRipple.Utilities.KnitUtils
import           BlueRipple.Utilities.TableUtils
import           BlueRipple.Data.DataFrames
import           BlueRipple.Data.PrefModel
import           BlueRipple.Data.PrefModel.SimpleAgeSexRace
import           BlueRipple.Data.PrefModel.SimpleAgeSexEducation
import qualified BlueRipple.Model.PreferenceBayes as PB
import qualified BlueRipple.Model.TurnoutAdjustment as TA


modeledResults :: ( MonadIO (K.Sem r)
                  , K.KnitEffects r
                  , Show tr
                  , Show b
                  , Enum b
                  , Bounded b
                  , A.Ix b
                  , FL.Vector (F.VectorFor b) b)
               => DemographicStructure dr tr HouseElections b
               -> (F.Record LocationKey -> Bool)
               -> F.Frame dr
               -> F.Frame tr
               -> F.Frame HouseElections
               -> M.Map Int Int
               -> K.Sem r (M.Map Int (PreferenceResults b FV.ParameterEstimate))
modeledResults ds locFilter dFrame tFrame eFrame years = flip traverse years $ \y -> do
  K.logLE K.Info $ "inferring " <> show (dsCategories ds) <> " for " <> show y
  preferenceModel ds locFilter y dFrame eFrame tFrame

-- PreferenceResults to list of group names and predicted D votes
-- But we want them as a fraction of D/D+R
data VoteShare = ShareOfAll | ShareOfD

modeledDVotes :: forall b. (A.Ix b, Bounded b, Enum b, Show b)
  => VoteShare -> PreferenceResults b Double -> [(T.Text, Double)]
modeledDVotes vs pr =
  let
    summed = FL.fold
             (votesAndPopByDistrictF @b)
             (F.rcast <$> votesAndPopByDistrict pr)
    popArray =
      F.rgetField @(PopArray b) summed
    turnoutArray =
      F.rgetField @(TurnoutArray b) summed
    predVoters = zipWith (*) (A.elems turnoutArray) $ fmap realToFrac (A.elems popArray)
    allDVotes  = F.rgetField @DVotes summed
    allRVotes  = F.rgetField @RVotes summed
    dVotes b =
      realToFrac (popArray A.! b)
      * (turnoutArray A.! b)
      * (modeled pr A.! b)
    allPredictedD = FL.fold FL.sum $ fmap dVotes Relude.universe --[minBound..maxBound]
    scale = case vs of
      ShareOfAll -> (realToFrac allDVotes/realToFrac (allDVotes + allRVotes))/allPredictedD
      ShareOfD -> 1/allPredictedD
  in
    fmap (\b -> (show b, scale * dVotes b))
    [(minBound :: b) .. maxBound]


data DeltaTableRow =
  DeltaTableRow
  { dtrGroup :: T.Text
  , dtrPop :: Int
  , dtrFromPop :: Int
  , dtrFromTurnout :: Int
  , dtrFromOpinion :: Int
  , dtrTotal :: Int
  , dtrPct :: Double
  } deriving (Show)

deltaTable
  :: forall dr tr e b r
   . (A.Ix b
     , Bounded b
     , Enum b
     , Show b
     , MonadIO (K.Sem r)
     , K.KnitEffects r
     )
  => DemographicStructure dr tr e b
  -> (F.Record LocationKey -> Bool)
  -> F.Frame e
  -> Int -- ^ year A
  -> Int -- ^ year B
  -> PreferenceResults b FV.ParameterEstimate
  -> PreferenceResults b FV.ParameterEstimate
  -> K.Sem r ([DeltaTableRow], (Int, Int), (Int, Int))
deltaTable ds locFilter electionResultsFrame yA yB trA trB = do
  let
    groupNames = show <$> dsCategories ds
    getPopAndTurnout
      :: Int -> PreferenceResults b FV.ParameterEstimate -> K.Sem r (A.Array b Int, A.Array b Double)
    getPopAndTurnout y tr = do
      resultsFrame <- knitX $ dsPreprocessElectionData ds y electionResultsFrame
      let
        totalDRVotes =
          let filteredResultsF = F.filterFrame (locFilter . F.rcast) resultsFrame
          in FL.fold (FL.premap (\r -> F.rgetField @DVotes r + F.rgetField @RVotes r) FL.sum) filteredResultsF
        totalRec = FL.fold
          votesAndPopByDistrictF
          ( fmap
              (F.rcast
                @'[PopArray b, TurnoutArray b, DVotes, RVotes]
              )
          $ F.filterFrame (locFilter . F.rcast)
          $ F.toFrame
          $ votesAndPopByDistrict tr
          )
        totalCounts = F.rgetField @(PopArray b) totalRec
        unAdjTurnout = nationalTurnout tr
      tDelta <- liftIO $ TA.findDeltaA totalDRVotes totalCounts unAdjTurnout
      let adjTurnout = TA.adjTurnoutP tDelta unAdjTurnout
      return (totalCounts, adjTurnout)

  (popA, turnoutA) <- getPopAndTurnout yA trA
  (popB, turnoutB) <- getPopAndTurnout yB trB
{-  K.logLE K.Info $ (T.pack $ show yA) <> "->" <> (T.pack $ show yB)
  K.logLE K.Info $ T.pack $ show turnoutA
  K.logLE K.Info $ T.pack $ show turnoutB -}
  let
    pop        = FL.fold FL.sum popA
    probsArray = fmap FV.value . modeled
    probA      = probsArray trA
    probB      = probsArray trB
    modeledVotes popArray turnoutArray probArray =
      let dVotes b =
              round
                $ realToFrac (popArray A.! b)
                * (turnoutArray A.! b)
                * (probArray A.! b)
          rVotes b =
              round
                $ realToFrac (popArray A.! b)
                * (turnoutArray A.! b)
                * (1.0 - probArray A.! b)
      in  FL.fold
            ((,) <$> FL.premap dVotes FL.sum <*> FL.premap rVotes FL.sum) Relude.universe
    makeDTR b =
      let pop0     = realToFrac $ popA A.! b
          dPop     = realToFrac $ (popB A.! b) - (popA A.! b)
          turnout0 = realToFrac $ turnoutA A.! b
          dTurnout = realToFrac $ (turnoutB A.! b) - (turnoutA A.! b)
          prob0    = realToFrac (probA A.! b)
          dProb    = realToFrac $ (probB A.! b) - (probA A.! b)
          dtrCombo = dPop * dTurnout * (2 * dProb) / 4 -- the rest is accounted for in other terms, we spread this among them
          dtrN =
              round
                $ dPop
                * (turnout0 + dTurnout / 2)
                * (2 * (prob0 + dProb / 2) - 1)
                + (dtrCombo / 3)
          dtrT =
              round
                $ (pop0 + dPop / 2)
                * dTurnout
                * (2 * (prob0 + dProb / 2) - 1)
                + (dtrCombo / 3)
          dtrO =
              round
                $ (pop0 + dPop / 2)
                * (turnout0 + dTurnout / 2)
                * (2 * dProb)
                + (dtrCombo / 3)
          dtrTotal = dtrN + dtrT + dtrO
      in  DeltaTableRow (show b)
                        (popB A.! b)
                        dtrN
                        dtrT
                        dtrO
                        dtrTotal
                        (realToFrac dtrTotal / realToFrac pop)
    groupRows = fmap makeDTR [minBound ..]
    addRow (DeltaTableRow g p fp ft fo t _) (DeltaTableRow _ p' fp' ft' fo' t' _)
      = DeltaTableRow g
                      (p + p')
                      (fp + fp')
                      (ft + ft')
                      (fo + fo')
                      (t + t')
                      (realToFrac (t + t') / realToFrac (p + p'))
    totalRow = FL.fold
      (FL.Fold addRow (DeltaTableRow "Total" 0 0 0 0 0 0) id)
      groupRows
    dVotesA = modeledVotes popA turnoutA probA
    dVotesB = modeledVotes popB turnoutB probB
  return (groupRows ++ [totalRow], dVotesA, dVotesB)

deltaTableColonnade :: C.Colonnade C.Headed DeltaTableRow T.Text
deltaTableColonnade =
  C.headed "Group" dtrGroup
    <> C.headed "Population (k)" (show . (`div` 1000) . dtrPop)
    <> C.headed "+/- From Population (k)"
                (show . (`div` 1000) . dtrFromPop)
    <> C.headed "+/- From Turnout (k)"
                (show . (`div` 1000) . dtrFromTurnout)
    <> C.headed "+/- From Opinion (k)"
                (show . (`div` 1000) . dtrFromOpinion)
    <> C.headed "+/- Total (k)" (show . (`div` 1000) . dtrTotal)
    <> C.headed "+/- %Vote" (toText @String . PF.printf "%2.2f" . (* 100) . dtrPct)

deltaTableColonnadeBlaze :: CellStyle DeltaTableRow T.Text -> C.Colonnade C.Headed DeltaTableRow BC.Cell
deltaTableColonnadeBlaze cas =
  C.headed "Group" (toCell cas "Group" "" (textToStyledHtml . dtrGroup))
  <> C.headed "Population (k)" (toCell cas "Population" "Population" (textToStyledHtml . show . (`div` 1000) . dtrPop))
  <>  C.headed "+/- From Population (k)"
  (toCell cas "FromPop" "+/- From Population" (numberToStyledHtml "%d" . (`div` 1000) . dtrFromPop))
  <> C.headed "+/- From Turnout (k)"
  (toCell cas "FromTurnout" "+/- From Turnout" (numberToStyledHtml "%d" . (`div` 1000) . dtrFromTurnout))
  <> C.headed "+/- From Opinion (k)"
  (toCell cas "FromOpinion" "+/- From Opinion" (numberToStyledHtml "%d" .  (`div` 1000) . dtrFromOpinion))
--     (\tr -> (numberCell "%.0d" (opinionHighlight (dtrGroup tr)) . (`div` 1000) $ dtrFromOpinion tr))
  <> C.headed "+/- Total (k)" (toCell cas "Total" "Total" (numberToStyledHtml "%d" . (`div` 1000) . dtrTotal))
  <> C.headed "+/- %Vote" (toCell cas "PctVote" "% Vote" (numberToStyledHtml "%2.2f" . (* 100) . dtrPct))

type X = "X" F.:-> Double
type ScaledDVotes = "ScaledDVotes" F.:-> Int
type ScaledRVotes = "ScaledRVotes" F.:-> Int
type PopArray b = "PopArray" F.:-> A.Array b Int
type TurnoutArray b = "TurnoutArray" F.:-> A.Array b Double

votesAndPopByDistrictF
  :: forall b
   . (A.Ix b, Bounded b, Enum b)
  => FL.Fold
       (F.Record '[PopArray b, TurnoutArray b, DVotes, RVotes])
       (F.Record '[PopArray b, TurnoutArray b, DVotes, RVotes])
votesAndPopByDistrictF =
  let voters r = A.listArray (minBound, maxBound)
                 $ zipWith (*) (A.elems $ F.rgetField @(TurnoutArray b) r) (fmap realToFrac $ A.elems $ F.rgetField @(PopArray b) r)
      g r = A.listArray (minBound, maxBound)
            $ zipWith (/) (A.elems $ F.rgetField @(TurnoutArray b) r) (fmap realToFrac $ A.elems $ F.rgetField @(PopArray b) r)
      recomputeTurnout r = F.rputField @(TurnoutArray b) (g r) r
  in PF.dimap (F.rcast @'[PopArray b, TurnoutArray b, DVotes, RVotes]) recomputeTurnout
    $    FF.sequenceRecFold
    $    FF.FoldRecord (PF.dimap (F.rgetField @(PopArray b)) V.Field FE.sumTotalNumArray)
    V.:& FF.FoldRecord (PF.dimap voters V.Field FE.sumTotalNumArray)
    V.:& FF.FoldRecord (PF.dimap (F.rgetField @DVotes) V.Field FL.sum)
    V.:& FF.FoldRecord (PF.dimap (F.rgetField @RVotes) V.Field FL.sum)
    V.:& V.RNil


data Aggregation c b where
  Aggregation :: (Enum b, Bounded b, Eq b, Ord b, A.Ix b,
                  Enum c, Bounded c, Eq c, Ord c, A.Ix c) => (c -> [b]) -> Aggregation c b


aggregateFold :: forall c b a x. Aggregation c b -> FL.Fold a x -> A.Array b a -> A.Array c x
aggregateFold (Aggregation children) fld arr =
  let cs = Relude.universe
      expanded :: [[a]]
      expanded = fmap (fmap (arr A.!) . children) cs
      folded = fmap (FL.fold fld) expanded
  in A.listArray (minBound,maxBound) folded

aggregateFold2 :: Aggregation c b -> FL.Fold (a,w) x -> A.Array b a -> A.Array b w -> A.Array c x
aggregateFold2 (Aggregation children) fld arrA arrW =
  let cs = Relude.universe
      as = fmap (fmap (arrA A.!) . children) cs
      ws = fmap (fmap (arrW A.!) . children) cs
      folded = FL.fold fld . uncurry zip <$> zip as ws
  in A.listArray (minBound, maxBound) folded

weightedFold :: (Num a, Fractional a) => FL.Fold (a,a) a
weightedFold =
  let sumWeightsF = FL.premap snd FL.sum
      weightedSumF = FL.premap (uncurry (*)) FL.sum
  in fmap (uncurry (/)) $ (,) <$> weightedSumF <*> sumWeightsF

popWeightedAggregate :: (Num d, Fractional d) => Aggregation c b -> A.Array b Int -> A.Array b d -> A.Array c d
popWeightedAggregate agg popArray datArray = aggregateFold2 agg weightedFold datArray (fmap realToFrac popArray)

aggregateRecord
  :: forall b c. Aggregation c b
  -> F.Record [StateAbbreviation
              , CongressionalDistrict
              , PopArray b -- population by group
              , TurnoutArray b -- adjusted turnout by group
              , DVotes
              , RVotes
              ]
  -> F.Record [StateAbbreviation
              , CongressionalDistrict
              , PopArray c -- population by group
              , TurnoutArray c -- adjusted turnout by group
              , DVotes
              , RVotes
              ]
aggregateRecord agg r =
  let
    f :: F.Record [PopArray b, TurnoutArray b] -> F.Record [PopArray c, TurnoutArray c]
    f x =
      let popB = F.rgetField @(PopArray b) x
          turnoutB = F.rgetField @(TurnoutArray b) x
      in aggregateFold agg FL.sum popB F.&: popWeightedAggregate agg popB turnoutB F.&: V.RNil
  in F.rcast $ FT.transform f r

cByB :: Aggregation c b -> LA.Matrix Double
cByB (Aggregation children) =
  let allBs = Relude.universe
      toZeroOne y = if y then 1 else 0
      getCol c = LA.fromList $ fmap (toZeroOne . (`elem` children c)) allBs
  in LA.fromColumns $ fmap getCol Relude.universe

aggregatePreferenceResults :: Fractional a => Aggregation c b -> PreferenceResults b a -> PreferenceResults c a
aggregatePreferenceResults agg pr =
  let prVBPAD' = fmap (aggregateRecord agg) (votesAndPopByDistrict pr)
      prTurnout' = popWeightedAggregate agg (nationalVoters pr) (nationalTurnout pr)
      prVoters' = aggregateFold agg FL.sum (nationalVoters pr)
      prModeled' = popWeightedAggregate agg (nationalVoters pr) (modeled pr)
      mCB = cByB agg
      prCovar' = LA.tr mCB LA.<> covariances pr LA.<> mCB
  in PreferenceResults prVBPAD' prTurnout' prVoters' prModeled' prCovar'


data PreferenceResults b a = PreferenceResults
  {
    votesAndPopByDistrict :: [F.Record [ StateAbbreviation
                                       , CongressionalDistrict
                                       , PopArray b -- population by group
                                       , TurnoutArray b -- adjusted turnout by group
                                       , DVotes
                                       , RVotes
                                       ]]
    , nationalTurnout :: A.Array b Double
    , nationalVoters :: A.Array b Int
    , modeled :: A.Array b a
    , covariances :: LA.Matrix Double
  }

instance Functor (PreferenceResults b) where
  fmap f (PreferenceResults v nt nv m c) = PreferenceResults v nt nv (fmap f m) c

preferenceModel
  :: forall dr tr b r
   . ( Show tr
     , Show b
     , Enum b
     , Bounded b
     , A.Ix b
     , FL.Vector (F.VectorFor b) b
     , K.KnitEffects r
     , MonadIO (K.Sem r)
     )
  => DemographicStructure dr tr HouseElections b
  -> (F.Record LocationKey -> Bool)
  -> Int
  -> F.Frame dr
  -> F.Frame HouseElections
  -> F.Frame tr
  -> K.Sem
       r
       (PreferenceResults b FV.ParameterEstimate)
preferenceModel ds locFilter year identityDFrame houseElexFrame turnoutFrame =
  do
    -- reorganize data from loaded Frames
    resultsFlattenedFrameFull <- knitX
      $ dsPreprocessElectionData ds year houseElexFrame
    let resultsFlattenedFrame = F.filterFrame (locFilter . F.rcast) resultsFlattenedFrameFull
    filteredTurnoutFrame <- knitX
      $ dsPreprocessTurnoutData ds year turnoutFrame
    let year' = year --if (year == 2018) then 2017 else year -- we're using 2017 for now, until census updated ACS data
    longByDCategoryFrame <- knitX
      $ dsPreprocessDemographicData ds year' identityDFrame

    -- turn long-format data into Arrays by demographic category, beginning with national turnout
    turnoutByGroupArray <-
      K.knitMaybe "Missing or extra group in turnout data?" $ FL.foldM
        (FE.makeArrayMF (F.rgetField @(DemographicCategory b))
                        (F.rgetField @VotedPctOfAll)
                        (flip const)
        )
        filteredTurnoutFrame

    -- now the populations in each district
    let votersArrayMF = MR.mapReduceFoldM
          (MR.generalizeUnpack MR.noUnpack)
          (MR.generalizeAssign $ MR.splitOnKeys @LocationKey)
          (MR.foldAndLabelM
            (fmap (FT.recordSingleton @(PopArray b))
                  (FE.recordsToArrayMF @(DemographicCategory b) @PopCount)
            )
            V.rappend
          )
    -- F.Frame (LocationKey V.++ (PopArray b))
    populationsFrame <-
      K.knitMaybe "Error converting long demographic data to arrays!"
      $   F.toFrame
      <$> FL.foldM votersArrayMF longByDCategoryFrame

    -- and the total populations in each group
    let addArray :: (A.Ix k, Num a) => A.Array k a -> A.Array k a -> A.Array k a
        addArray a1 a2 = A.accum (+) a1 (A.assocs a2)
        zeroArray :: (A.Ix k, Bounded k, Enum k, Num a) => A.Array k a
        zeroArray = A.listArray (minBound, maxBound) $ L.repeat 0
        popByGroupArray = FL.fold (FL.premap (F.rgetField @(PopArray b)) (FL.Fold addArray zeroArray id)) populationsFrame

    let
      resultsWithPopulationsFrame =
        catMaybes $ fmap F.recMaybe $ F.leftJoin @LocationKey resultsFlattenedFrame
                                                              populationsFrame

    K.logLE K.Info "Computing Ghitza-Gelman turnout adjustment for each district so turnouts produce correct number D+R votes."
    resultsWithPopulationsAndGGAdjFrame <- fmap F.toFrame $ flip traverse resultsWithPopulationsFrame $ \r -> do
      let tVotesF x = F.rgetField @DVotes x + F.rgetField @RVotes x -- Should this be D + R or total?
      ggDelta <- ggTurnoutAdj r tVotesF turnoutByGroupArray
      K.logLE K.Diagnostic $
        "Ghitza-Gelman turnout adj="
        <> show ggDelta
        <> "; Adj Turnout=" <> show (TA.adjTurnoutP ggDelta turnoutByGroupArray)
      return $ FT.mutate (const $ FT.recordSingleton @(TurnoutArray b) $ TA.adjTurnoutP ggDelta turnoutByGroupArray) r

    let onlyOpposed r =
          (F.rgetField @DVotes r > 0) && (F.rgetField @RVotes r > 0)
        opposedFrame = F.filterFrame onlyOpposed resultsWithPopulationsAndGGAdjFrame
        numCompetitiveRaces = FL.fold FL.length opposedFrame

    K.logLE K.Info
      $ "After removing races where someone is running unopposed we have "
      <> show numCompetitiveRaces
      <> " contested races."

    totalVoteDiagnostics @b resultsWithPopulationsAndGGAdjFrame opposedFrame


    let
      scaleInt s n = round $ s * realToFrac n
      mcmcData =
        fmap
        (\r ->
           ( F.rgetField @DVotes r
           , VB.fromList $ fmap round (adjVotersL (F.rgetField @(TurnoutArray b) r) (F.rgetField @(PopArray b) r))
           )
        )
        $ FL.fold FL.list opposedFrame
      numParams = length $ dsCategories ds
    (cgRes, _, _) <- liftIO $ PB.cgOptimizeAD mcmcData (VB.fromList $ (const 0.5) <$> dsCategories ds)
    let cgParamsA = A.listArray (minBound :: b, maxBound) $ VB.toList cgRes
        cgVarsA = A.listArray (minBound :: b, maxBound) $ VS.toList $ PB.variances mcmcData cgRes
        npe cl b =
          let
            x = cgParamsA A.! b
            sigma = sqrt $ cgVarsA A.! b
            dof = realToFrac $ numCompetitiveRaces - L.length (A.elems cgParamsA)
            interval = S.quantile (S.studentTUnstandardized dof 0 sigma) (1.0 - (S.significanceLevel cl/2))
          in FV.ParameterEstimate x (x - interval/2.0, x + interval/2.0)
--          in FV.NamedParameterEstimate (T.pack $ show b) pEstimate
        parameterEstimatesA = A.listArray (minBound :: b, maxBound) $ fmap (npe S.cl95) Relude.universe

    K.logLE K.Info $ "MLE results: " <> show (A.elems parameterEstimatesA)
-- For now this bit is diagnostic.  But we should chart the correlations
-- and, perhaps, the eigenvectors of the covariance??
    let cgCovar = PB.covar mcmcData cgRes -- TODO: make a chart out of this
        (cgEv, cgEvs) = PB.mleCovEigens mcmcData cgRes
    K.logLE K.Diagnostic $ "sigma = " <> show (fmap sqrt $ cgVarsA)
    K.logLE K.Diagnostic $ "Covariances=" <> toText (PB.disps 3 cgCovar)
    K.logLE K.Diagnostic $ "Correlations=" <> toText (PB.disps 3 $ PB.correlFromCov cgCovar)
    K.logLE K.Diagnostic $ "Eigenvalues=" <> show cgEv
    K.logLE K.Diagnostic $ "Eigenvectors=" <> toText (PB.disps 3 cgEvs)

    return $ PreferenceResults
      (F.rcast <$> FL.fold FL.list opposedFrame)
      turnoutByGroupArray
      popByGroupArray
      parameterEstimatesA
      cgCovar

ggTurnoutAdj :: forall b rs r. (A.Ix b
                               , F.ElemOf rs (PopArray b)
                               , MonadIO (K.Sem r)
                               ) => F.Record rs -> (F.Record rs -> Int) -> A.Array b Double -> K.Sem r Double
ggTurnoutAdj r totalVotesF unadjTurnoutP = do
  let population = F.rgetField @(PopArray b) r
      totalVotes = totalVotesF r
  liftIO $ TA.findDeltaA totalVotes population unadjTurnoutP

adjVotersL :: A.Array b Double -> A.Array b Int -> [Double]
adjVotersL turnoutPA popA = zipWith (*) (A.elems turnoutPA) (realToFrac <$> A.elems popA)

totalVoteDiagnostics :: forall b rs f r
                        . (A.Ix b
                          , Foldable f
                          , F.ElemOf rs (PopArray b)
                          , F.ElemOf rs (TurnoutArray b)
                          , F.ElemOf rs Totalvotes
                          , F.ElemOf rs DVotes
                          , F.ElemOf rs RVotes
                          , K.KnitEffects r
                        )
  => f (F.Record rs) -- ^ frame with all rows
  -> f (F.Record rs) -- ^ frame with only rows from competitive races
  -> K.Sem r ()
totalVoteDiagnostics allFrame opposedFrame = K.wrapPrefix "VoteSummary" $ do
  let allVoters r = FL.fold FL.sum
                    $ zipWith (*) (A.elems $ F.rgetField @(TurnoutArray b) r) (fmap realToFrac $ A.elems $ F.rgetField @(PopArray b) r)
      allVotersF = FL.premap allVoters FL.sum
      allVotesF  = FL.premap (F.rgetField @Totalvotes) FL.sum
      allDVotesF = FL.premap (F.rgetField @DVotes) FL.sum
      allRVotesF = FL.premap (F.rgetField @RVotes) FL.sum
  --      allDRVotesF = FL.premap (\r -> F.rgetField @DVotes r + F.rgetField @RVotes r) FL.sum
      (totalVoters, totalVotes, totalDVotes, totalRVotes) = FL.fold
        ((,,,) <$> allVotersF <*> allVotesF <*> allDVotesF <*> allRVotesF)
        allFrame
      (totalVotersCD, totalVotesCD, totalDVotesCD, totalRVotesCD) = FL.fold
        ((,,,) <$> allVotersF <*> allVotesF <*> allDVotesF <*> allRVotesF)
        opposedFrame
  K.logLE K.Info $ "voters=" <> show totalVoters
  K.logLE K.Info $ "house votes=" <> show totalVotes
  K.logLE K.Info
    $  "D/R/D+R house votes="
    <> show totalDVotes
    <> "/"
    <> show totalRVotes
    <> "/"
    <> show (totalDVotes + totalRVotes)
  K.logLE K.Info
    $  "voters (competitive districts)="
    <> show totalVotersCD
  K.logLE K.Info
    $  "house votes (competitive districts)="
    <> show totalVotesCD
  K.logLE K.Info
    $  "D/R/D+R house votes (competitive districts)="
    <> show totalDVotesCD
    <> "/"
    <> show totalRVotesCD
    <> "/"
    <> show (totalDVotesCD + totalRVotesCD)


totalArrayZipWith :: (A.Ix b, Enum b, Bounded b)
                  => (x -> y -> z)
                  -> A.Array b x
                  -> A.Array b y
                  -> A.Array b z
totalArrayZipWith f xs ys = A.listArray (minBound, maxBound) $ zipWith f (A.elems xs) (A.elems ys)

vlGroupingChart :: Foldable f
                => T.Text
                -> FV.ViewConfig
                -> f (F.Record ['("Group", T.Text)
                               ,'("VotingAgePop", Int)
                               ,'("Turnout",Double)
                               ,'("Voters", Int)
                               ,'("D Voter Preference", Double)
                               ])
                -> GV.VegaLite
vlGroupingChart title vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows
      xLabel = "Inferred (%) Likelihood of Voting Democratic"
      estimateXenc = GV.position GV.X [FV.pName @'("D Voter Preference", Double)
                                      ,GV.PmType GV.Quantitative
                                      ,GV.PAxis [GV.AxTitle xLabel]
                                      ]
      estimateYenc = GV.position GV.Y [FV.pName @'("Group",T.Text)
                                      ,GV.PmType GV.Ordinal
                                      ,GV.PAxis [GV.AxTitle "Demographic Group"]
                                      ]
      estimateSizeEnc = GV.size [FV.mName @'("Voters",Int)
                                , GV.MmType GV.Quantitative
                                , GV.MScale [GV.SDomain $ GV.DNumbers [5e6,30e6]]
                                , GV.MLegend [GV.LFormatAsNum]

                                ]
      estimateColorEnc = GV.color [FV.mName @'("Turnout", Double)
                                  , GV.MmType GV.Quantitative
                                  , GV.MScale [GV.SDomain $ GV.DNumbers [0.2,0.8]
                                              ,GV.SScheme "blues" [0.3,1.0]
                                              ]
                                  , GV.MLegend [GV.LGradientLength (vcHeight vc / 3)
                                               , GV.LFormatAsNum
                                               , GV.LFormat "%"
                                               ]
                                  ]
      estEnc = estimateXenc . estimateYenc . estimateSizeEnc . estimateColorEnc
      estSpec = GV.asSpec [(GV.encoding . estEnc) [], GV.mark GV.Point [GV.MFilled True]]
  in
    FV.configuredVegaLite vc [FV.title title, GV.layer [estSpec], dat]

exitCompareChart :: Foldable f
                 => T.Text
                 -> FV.ViewConfig
                 -> f (F.Record ['("Group", T.Text)
                                ,'("Model Dem Pref", Double)
                                ,'("ModelvsExit",Double)
                               ])
                -> GV.VegaLite
exitCompareChart title vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows
      xLabel = "Modeled % Likelihood of Voting Democratic"
      xEnc =  GV.position GV.X [FV.pName @'("Model Dem Pref", Double)
                               ,GV.PmType GV.Quantitative
                               ,GV.PAxis [GV.AxTitle xLabel
                                         , GV.AxFormatAsNum
                                         , GV.AxFormat "%"
                                         ]
                               ]
      yEnc = GV.position GV.Y [FV.pName @'("ModelvsExit", Double)
                              ,GV.PmType GV.Quantitative
                              ,GV.PScale [GV.SDomain $ GV.DNumbers [negate 0.15,0.15]]
                              ,GV.PAxis [GV.AxTitle "Model - Exit Poll"
                                        , GV.AxFormatAsNum
                                        , GV.AxFormat "%"
                                        ]
                              ]
      colorEnc = GV.color [FV.mName @'("Group", T.Text)
                          , GV.MmType GV.Nominal
                          ]
      enc = xEnc . yEnc . colorEnc
      spec = GV.asSpec [(GV.encoding . enc) [], GV.mark GV.Point [GV.MFilled True, GV.MSize 100]]
  in
    FV.configuredVegaLite vc [FV.title title, GV.layer [spec], dat]



vlGroupingChartExit :: Foldable f
                    => T.Text
                    -> FV.ViewConfig
                    -> f (F.Record ['("Group", T.Text)
                                   ,'("VotingAgePop", Int)
                                   ,'("Voters", Int)
                                   ,'("D Voter Preference", Double)
                                   ,'("InfMinusExit", Double)
                                   ])
                    -> GV.VegaLite
vlGroupingChartExit title vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows
      xLabel = "Inferred Likelihood of Voting Democratic"
      estimateXenc = GV.position GV.X [FV.pName @'("D Voter Preference", Double)
                                      ,GV.PmType GV.Quantitative
                                      ,GV.PAxis [GV.AxTitle xLabel]
                                      ]
      estimateYenc = GV.position GV.Y [FV.pName @'("Group",T.Text)
                                      ,GV.PmType GV.Ordinal
                                      ]
      estimateSizeEnc = GV.size [FV.mName @'("VotingAgePop",Int)
                                , GV.MmType GV.Quantitative]
      estimateColorEnc = GV.color [FV.mName @'("InfMinusExit", Double)
                                  , GV.MmType GV.Quantitative]
      estEnc = estimateXenc . estimateYenc . estimateSizeEnc . estimateColorEnc
      estSpec = GV.asSpec [(GV.encoding . estEnc) [], GV.mark GV.Point []]
  in
    FV.configuredVegaLite vc [FV.title title, GV.layer [estSpec], dat]
