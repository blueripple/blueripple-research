{-# LANGUAGE DataKinds      #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module BlueRipple.Data.GeographicTypes
  (
    module BlueRipple.Data.GeographicTypes
--  , module BlueRipple.Data.DataFrames
  )
  where

import qualified BlueRipple.Data.Keyed         as K
import Data.Discrimination (Grouping)
import qualified Data.Text.Read as TR
import qualified Data.Vector.Unboxed as UVec
import Data.Vector.Unboxed.Deriving (derivingUnbox)
import qualified Data.Vinyl as V
import qualified Flat
import qualified Frames as F
import qualified Frames.Streamly.InCore as FSI
import qualified Frames.Streamly.TH as FTH
import qualified Frames.ShowCSV as FCSV
import qualified Frames.Visualization.VegaLite.Data as FV
import qualified Graphics.Vega.VegaLite as GV

import qualified Text.Show

FTH.declareColumn "StateName" ''Text

FTH.declareColumn "StateAbbreviation" ''Text

FTH.declareColumn "StateFIPS" ''Int

FTH.declareColumn "CountyFIPS" ''Int

FTH.declareColumn "PUMA" ''Int

FTH.declareColumn "CongressionalDistrict" ''Int

data DistrictType = Congressional | StateUpper | StateLower deriving stock (Show, Enum, Bounded, Eq, Ord, Generic)

--instance S.Serialize DistrictType
--instance B.Binary DistrictType
instance Flat.Flat DistrictType
instance Grouping DistrictType
instance FCSV.ShowCSV DistrictType
derivingUnbox
  "DistrictType"
  [t|DistrictType -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]

type instance FSI.VectorFor DistrictType = UVec.Vector
FTH.declareColumn "DistrictTypeC" ''DistrictType

instance FV.ToVLDataValue (F.ElField DistrictTypeC) where
  toVLDataValue x = (toText $ V.getLabel x, GV.Str $ show $ V.getField x)

--FS.declareColumn "DistrictNumber" ''Int
FTH.declareColumn "DistrictName" ''Text

-- if integer part and text part, compare using integer part first, then text.  Else just use text.
districtNameCompare :: Text -> Text -> Ordering
districtNameCompare t1 t2 = --if compInt == EQ then compare tr1 tr2 else compInt where
  let parsed1 = TR.decimal t1
      parsed2 = TR.decimal t2
  in case (parsed1, parsed2) of
    (Right (n1 :: Int, tr1), Right (n2, tr2)) -> if n1 == n2 then compare tr1 tr2 else compare n1 n2
    _ -> compare t1 t2
{-# INLINEABLE districtNameCompare #-}

data LegislativeDistrict = LegislativeDistrict { ldType :: DistrictType, ldName :: Text} deriving (Eq)

instance Show LegislativeDistrict where
  show (LegislativeDistrict t n) = show t <> "-" <> toString n

instance Ord LegislativeDistrict where
  compare (LegislativeDistrict t1 n1) (LegislativeDistrict t2 n2) = compare t1 t2 <> districtNameCompare n1 n2

-- Serialize for caching
-- FI.VectorFor for frames
-- Grouping for leftJoin
data CensusRegion = Northeast | Midwest | South | West | OtherRegion deriving stock (Show, Enum, Bounded, Eq, Ord, Generic)
deriving anyclass instance Hashable CensusRegion

--instance S.Serialize CensusRegion
--instance B.Binary CensusRegion
instance Flat.Flat CensusRegion
instance Grouping CensusRegion
instance K.FiniteSet CensusRegion
derivingUnbox "CensusRegion"
  [t|CensusRegion -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FSI.VectorFor CensusRegion = UVec.Vector

--type CensusRegionC = "CensusRegion" F.:-> CensusRegion
FTH.declareColumn "CensusRegionC" ''CensusRegion

data CensusDivision = NewEngland
                    | MiddleAtlantic
                    | EastNorthCentral
                    | WestNorthCentral
                    | SouthAtlantic
                    | EastSouthCentral
                    | WestSouthCentral
                    | Mountain
                    | Pacific
                    | OtherDivision
                  deriving stock (Show, Enum, Bounded, Eq, Ord, Generic)
deriving anyclass instance Hashable CensusDivision

--instance S.Serialize CensusDivision
--instance B.Binary CensusDivision
instance Flat.Flat CensusDivision
instance Grouping CensusDivision
instance K.FiniteSet CensusDivision
derivingUnbox "CensusDivision"
  [t|CensusDivision -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FSI.VectorFor CensusDivision = UVec.Vector

--type CensusDivisionC = "CensusDivision" F.:-> CensusDivision
FTH.declareColumn "CensusDivisionC" ''CensusDivision

censusDivisionToRegion :: CensusDivision -> CensusRegion
censusDivisionToRegion NewEngland = Northeast
censusDivisionToRegion MiddleAtlantic = Northeast
censusDivisionToRegion EastNorthCentral = Midwest
censusDivisionToRegion WestNorthCentral = Midwest
censusDivisionToRegion SouthAtlantic = South
censusDivisionToRegion EastSouthCentral = South
censusDivisionToRegion WestSouthCentral = South
censusDivisionToRegion Mountain = West
censusDivisionToRegion Pacific = West
censusDivisionToRegion OtherDivision = OtherRegion

data CensusMetro = MetroUnknown
                 | NonMetro
                 | MetroPrincipal
                 | MetroOther
                 | MetroMixed deriving stock (Show, Enum, Bounded, Eq, Ord, Generic)
deriving anyclass instance Hashable CensusMetro

--instance S.Serialize CensusMetro
--instance B.Binary CensusMetro
instance Flat.Flat CensusMetro
instance Grouping CensusMetro
instance K.FiniteSet CensusMetro
derivingUnbox "CensusMetro"
  [t|CensusMetro -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FSI.VectorFor CensusMetro = UVec.Vector

--type CensusMetroC = "CensusMetro" F.:-> CensusMetro
FTH.declareColumn "CensusMetroC" ''CensusMetro


--type PctInMetro = "PctIntMetro" F.:-> Double
FTH.declareColumn "PctInMetro" ''Double
