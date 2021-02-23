{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
module BlueRipple.Data.CensusLoaders where

import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.KeyedTables as KT
import qualified BlueRipple.Data.CensusTables as BRC
--import qualified BlueRipple.Data.Keyed as K

import qualified Data.Csv as CSV
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Frames                        as F
import qualified Frames.TH as F
import qualified Frames.InCore                 as FI
import qualified Data.Vinyl as V

sexByAgeKeyRec :: (DT.Sex, BRC.Age14) -> F.Record [BRC.Age14C, DT.SexC]
sexByAgeKeyRec (s, a) = a F.&: s F.&: V.RNil
{-# INLINE sexByAgeKeyRec #-}

raceBySexByAgeKeyRec :: (DT.RaceAlone4, (DT.Sex, BRC.Age14)) -> F.Record [BRC.Age14C, DT.SexC, DT.RaceAlone4C]
raceBySexByAgeKeyRec (r, (s, a)) = a F.&: s F.&: r F.&: V.RNil
{-# INLINE raceBySexByAgeKeyRec #-}

sexByCitizenshipKeyRec :: (DT.Sex, BRC.Citizenship) -> F.Record [DT.SexC, BRC.CitizenshipC]
sexByCitizenshipKeyRec (s, c) = s F.&: c F.&: V.RNil
{-# INLINE sexByCitizenshipKeyRec #-}

raceBySexByCitizenshipKeyRec :: (DT.RaceAlone4, (DT.Sex, BRC.Citizenship)) -> F.Record [DT.SexC, DT.RaceAlone4C, BRC.CitizenshipC]
raceBySexByCitizenshipKeyRec (r, (s, c)) = s F.&: r F.&: c F.&: V.RNil
{-# INLINE raceBySexByCitizenshipKeyRec #-}

sexByEducationKeyRec :: (DT.Sex, BRC.Education4) -> F.Record [DT.SexC, BRC.Education4C]
sexByEducationKeyRec (s, e) = s F.&: e F.&: V.RNil
{-# INLINE sexByEducationKeyRec #-}

raceBySexByEducationKeyRec :: (DT.RaceAlone4, (DT.Sex, BRC.Education4)) -> F.Record [DT.SexC, BRC.Education4C, DT.RaceAlone4C]
raceBySexByEducationKeyRec (r, (s, e)) = s F.&: e F.&: r F.&: V.RNil
{-# INLINE raceBySexByEducationKeyRec #-}
