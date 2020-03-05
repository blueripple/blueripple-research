{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
module BlueRipple.Data.ACS_PUMS_Loaders
  (
    pumsLoadAll
  , module BlueRipple.Data.ACS_PUMS_Loader.Pums2010
  , module BlueRipple.Data.ACS_PUMS_Loader.Pums2012
  , module BlueRipple.Data.ACS_PUMS_Loader.Pums2014
  , module BlueRipple.Data.ACS_PUMS_Loader.Pums2016
  , module BlueRipple.Data.ACS_PUMS_Loader.Pums2018  
  )
where

import BlueRipple.Data.ACS_PUMS_Loader.Pums2010
import BlueRipple.Data.ACS_PUMS_Loader.Pums2012
import BlueRipple.Data.ACS_PUMS_Loader.Pums2014
import BlueRipple.Data.ACS_PUMS_Loader.Pums2016
import BlueRipple.Data.ACS_PUMS_Loader.Pums2018
import BlueRipple.Data.ACS_PUMS (PUMS)
import qualified Knit.Report as K
import qualified Frames as F

pumsLoadAll :: K.KnitEffects r => K.Sem r (F.FrameRec PUMS)
pumsLoadAll = mconcat <$> sequence [pumsLoader2010, pumsLoader2012, pumsLoader2014, pumsLoader2016, pumsLoader2018]
