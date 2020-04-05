{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE Rank2Types          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE UndecidableInstances #-}

module BlueRipple.Data.CPSTurnoutPUMS.CPSTurnoutPUMS_Frame where
  ( module BlueRipple.Data.CPSTurnoutPUMS.CPSTurnout_Path
  , module BlueRipple.Data.CPSTurnoutPUMS.CPSTurnout_Frame
  )
where

import           BlueRipple.Data.CPSTurnoutPUMS.CPSTurnout_Path

import qualified Frames.TH                     as F

F.tableTypes' cpsTurnoutPUMSRowGen


